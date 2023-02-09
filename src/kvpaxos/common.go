package kvpaxos

import "hash/fnv"

const (
	OK               Err = "OK"
	ErrNoKey         Err = "ErrNoKey"
	ErrSubmitError   Err = "ErrSubmitError"
	ErrInvalidMethod Err = "ErrInvalidMethod"
	ErrDoSeqBehind   Err = "ErrDoSeqBehind"
)

type Err string

type PutArgs struct {
	// You'll have to add definitions here.
	Key    string
	Value  string
	DoHash bool // For PutHash
	// You'll have to add definitions here.
	CallID string
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutReply struct {
	Err           Err
	PreviousValue string // For PutHash
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	CallID string
}

type GetReply struct {
	Err   Err
	Value string
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
