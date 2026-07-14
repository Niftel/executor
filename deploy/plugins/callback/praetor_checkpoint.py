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

from ansible.plugins.callback import CallbackBase


class CallbackModule(CallbackBase):
    CALLBACK_VERSION = 2.0
    CALLBACK_TYPE = "notification"
    CALLBACK_NAME = "praetor_checkpoint"
    CALLBACK_NEEDS_ENABLED = True

    def __init__(self):
        super().__init__()
        self.path = os.environ.get("PRAETOR_CHECKPOINT", "checkpoint.json")
        self.state = {"resume_at": None, "vars": {}}
        self._load()

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

    def v2_runner_on_failed(self, result, ignore_errors=False):
        # A failed task can still register (e.g. with failed_when/ignore_errors);
        # keep whatever it produced so resume restores it.
        self._record_register(result)
