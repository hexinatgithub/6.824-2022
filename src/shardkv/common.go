package shardkv

import (
	"fmt"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrReject      = "ErrReject"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
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

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId  int64
	RequestId int64
}

func (a *GetArgs) String() string {
	return fmt.Sprintf(`GetArgs{Key: "%s", ClientId: %d, RequestId: %d}`, a.Key, a.ClientId, a.RequestId)
}

type GetReply struct {
	Err   Err
	Value string
}

type MigrationArgs struct {
	Source, Dest     int
	Num              int
	ShardWithSession []ShardWithSession
}

func (a *MigrationArgs) DeepCopy() MigrationArgs {
	c := MigrationArgs{Source: a.Source, Dest: a.Dest, Num: a.Num}
	for i := range a.ShardWithSession {
		c.ShardWithSession = append(c.ShardWithSession, a.ShardWithSession[i].DeepCopy())
	}
	return c
}

func (a *MigrationArgs) String() string {
	return fmt.Sprintf(`MigrationArgs{Source: %d, Dest: %d, Num: %d, ShardWithSession: %v}`, a.Source, a.Dest, a.Num, a.ShardWithSession)
}

type MigrationReply struct {
	Err Err
}

func (r *MigrationReply) String() string {
	return fmt.Sprintf("MigrationReply{Err: %s}", r.Err)
}
