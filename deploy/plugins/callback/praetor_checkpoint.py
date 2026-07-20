"""Praetor checkpoint callback plugin.

Records, into a JSON checkpoint file, enough state to resume a play after the
host running it restarts mid-execution:

  * resume_at  - the name of the task that was in progress (re-run on resume,
                 since a reboot leaves its completion unknown).
  * vars       - the value of every `register:`-ed result so far, so tasks after
                 the resume point can be restored with `-e @<vars>`.

The host-runner enables this on every play. On resume it reads the checkpoint
and relaunches with `--start-at-task="<resume_at>" -e @<vars>` so completed
tasks are skipped and registered state is restored.

Enable with:
  ANSIBLE_CALLBACK_PLUGINS=<this dir>
  ANSIBLE_CALLBACKS_ENABLED=praetor_checkpoint
  PRAETOR_CHECKPOINT=/var/lib/praetor/jobs/<run_id>/checkpoint.json
"""

from __future__ import annotations

import json
import os
import time

from ansible.plugins.callback import CallbackBase


class CallbackModule(CallbackBase):
    CALLBACK_VERSION = 2.0
    CALLBACK_TYPE = "notification"
    CALLBACK_NAME = "praetor_checkpoint"
    CALLBACK_NEEDS_ENABLED = True

    def __init__(self):
        super().__init__()
        self.path = os.environ.get("PRAETOR_CHECKPOINT", "checkpoint.json")
        self.events_path = os.environ.get("PRAETOR_DIAGNOSTIC_EVENTS")
        self.state = {"resume_at": None, "vars": {}}
        self.task_started = {}
        self._load()

    def _emit(self, event_type, **fields):
        """Append only explicitly allowlisted execution metadata.

        Ansible result dictionaries are deliberately never accepted here: they
        may contain module arguments, stdout, tokens, passwords, or other
        automation secrets.
        """
        if not self.events_path:
            return
        event = {
            "schema_version": 1,
            "event_type": event_type,
            "timestamp": time.time(),
        }
        allowed = (
            "play_name", "task_name", "task_uuid", "task_action", "host",
            "outcome", "changed", "duration_ms", "failure_code",
        )
        event.update({key: fields[key] for key in allowed if fields.get(key) is not None})
        try:
            with open(self.events_path, "a") as stream:
                stream.write(json.dumps(event, separators=(",", ":")) + "\n")
        except Exception:
            pass

    def _load(self):
        try:
            with open(self.path) as f:
                self.state = json.load(f)
        except Exception:
            pass
        self.state.setdefault("vars", {})

    def _save(self):
        tmp = self.path + ".tmp"
        try:
            with open(tmp, "w") as f:
                json.dump(self.state, f)
            os.replace(tmp, self.path)
        except Exception:
            pass

    def v2_playbook_on_task_start(self, task, is_conditional):
        # The task that is about to run. If the host reboots now, resume here:
        # the task may have been only partially applied, so it is re-run.
        self.state["resume_at"] = task.get_name()
        self._save()
        task_uuid = str(task._uuid)
        self.task_started[task_uuid] = time.monotonic()
        self._emit(
            "TASK_STARTED",
            task_name=task.get_name(),
            task_uuid=task_uuid,
            task_action=getattr(task, "action", None),
        )

    def v2_playbook_on_play_start(self, play):
        self._emit("PLAY_STARTED", play_name=play.get_name())

    def _emit_result(self, result, outcome, failure_code=None):
        task = result._task
        task_uuid = str(task._uuid)
        started = self.task_started.get(task_uuid)
        duration_ms = None if started is None else int((time.monotonic() - started) * 1000)
        self._emit(
            "HOST_" + outcome.upper(),
            task_name=task.get_name(),
            task_uuid=task_uuid,
            task_action=getattr(task, "action", None),
            host=result._host.get_name(),
            outcome=outcome,
            changed=bool(result._result.get("changed", False)),
            duration_ms=duration_ms,
            failure_code=failure_code,
        )

    def _record_register(self, result):
        register = getattr(result._task, "register", None)
        if not register:
            return
        try:
            json.dumps(result._result)  # only keep serializable results
        except (TypeError, ValueError):
            return
        self.state["vars"][register] = result._result
        self._save()

    def v2_runner_on_ok(self, result):
        self._record_register(result)
        self._emit_result(result, "changed" if result._result.get("changed") else "ok")

    def v2_runner_on_failed(self, result, ignore_errors=False):
        # A failed task can still register (e.g. with failed_when/ignore_errors);
        # keep whatever it produced so resume restores it.
        self._record_register(result)
        self._emit_result(result, "failed", "ignored" if ignore_errors else "task_failed")

    def v2_runner_on_unreachable(self, result):
        self._emit_result(result, "unreachable", "host_unreachable")

    def v2_runner_on_skipped(self, result):
        self._emit_result(result, "skipped")
