package kvraft

import "fmt"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	RequestId int64
}

func (a *PutAppendArgs) String() string {
	return fmt.Sprintf(`PutAppendArgs{Key: "%s", Value: "%s", Op: "%s", ClientId: %d, RequestId: %d}`, a.Key, a.Value, a.Op, a.ClientId, a.RequestId)
}

type PutAppendReply struct {
	Err Err
}

func (r *PutAppendReply) String() string {
	return fmt.Sprintf(`PutAppendReply{Err: "%s"}`, r.Err)
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId  int64
	RequestId int64
}

func (a *GetArgs) String() string {
	return fmt.Sprintf("GetArgs{Key: %s, ClientId: %d, RequestId: %d}", a.Key, a.ClientId, a.RequestId)
}

type GetReply struct {
	Err   Err
	Value string
}

func (r *GetReply) String() string {
	return fmt.Sprintf(`GetReply{Err: "%s", Value: "%s"}`, r.Err, r.Value)
}
