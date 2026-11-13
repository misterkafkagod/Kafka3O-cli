// Package messages reads and searches topic messages through the Console and shows them safely in a terminal.
package messages

import "time"

// Message is one record as the Gateway returns it; Key, Value and header values are the raw bytes (nil = null).
type Message struct {
	Partition   int32     `json:"partition"`
	Offset      int64     `json:"offset"`
	OffsetExact string    `json:"offsetExact"`
	Timestamp   time.Time `json:"timestamp"`
	Key         []byte    `json:"key"`
	Value       []byte    `json:"value"`
	Headers     []Header  `json:"headers"`
}

type Header struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

// Position is where reading starts: a partition and either an offset or a Unix-millisecond timestamp.
type Position struct {
	Partition int32
	Offset    *int64
	Timestamp *int64
}
