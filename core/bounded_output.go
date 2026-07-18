package core

import "unicode/utf8"

const (
	maxExecutorErrorBytes = 16 * 1024
	truncationMarker      = "\n... [truncated] ...\n"
)

// boundedErrorText preserves the beginning and diagnostic tail of an error while
// keeping executor-generated events comfortably below transport payload limits.
func boundedErrorText(value string) string {
	if len(value) <= maxExecutorErrorBytes {
		return value
	}
	budget := maxExecutorErrorBytes - len(truncationMarker)
	headBytes := budget / 4
	tailBytes := budget - headBytes
	head := validUTF8Head(value[:headBytes])
	tail := validUTF8Tail(value[len(value)-tailBytes:])
	return head + truncationMarker + tail
}

func validUTF8Head(value string) string {
	for len(value) > 0 && !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}

func validUTF8Tail(value string) string {
	for len(value) > 0 && !utf8.ValidString(value) {
		value = value[1:]
	}
	return value
}

// boundedTailBuffer implements io.Writer while retaining only the most recent
// bytes. SSH extraction tools often put their actionable failure at the end of a
// very noisy stderr stream.
type boundedTailBuffer struct {
	limit int
	data  []byte
}

func newBoundedTailBuffer(limit int) *boundedTailBuffer {
	return &boundedTailBuffer{limit: limit}
}

func (b *boundedTailBuffer) Write(p []byte) (int, error) {
	written := len(p)
	if b.limit <= 0 {
		return written, nil
	}
	if len(p) >= b.limit {
		b.data = append(b.data[:0], p[len(p)-b.limit:]...)
		return written, nil
	}
	overflow := len(b.data) + len(p) - b.limit
	if overflow > 0 {
		copy(b.data, b.data[overflow:])
		b.data = b.data[:len(b.data)-overflow]
	}
	b.data = append(b.data, p...)
	return written, nil
}

func (b *boundedTailBuffer) String() string {
	return validUTF8Tail(string(b.data))
}
