package pbservice

import "hash/fnv"

const (
	// Success Err
	OK       = "OK"
	ErrNoKey = "ErrNoKey"

	//Fail Err
	ErrNotMaster       = "ErrNotMaster"
	ErrNotBackup       = "ErrNotBackup"
	ErrSenderNotMaster = "ErrSenderNotMaster"
	ErrBackupTimeout   = "ErrBackupTimeout"
)

type Err string

type PutArgs struct {
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

// Your RPC definitions here.
type CloneArgs struct {
	Sender       string
	Store        map[string]string
	CompletedOps map[string]string
}

type CloneReply struct {
	Err Err
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
