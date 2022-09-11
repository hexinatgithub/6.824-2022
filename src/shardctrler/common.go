package shardctrler

import "fmt"

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (c Config) String() string {
	return fmt.Sprintf("Config{Num: %d, Shards: %v, Groups: %v}", c.Num, c.Shards, c.Groups)
}

func (c Config) DeepCopy() Config {
	config := Config{
		Num:    c.Num,
		Shards: c.Shards,
		Groups: make(map[int][]string),
	}
	for k := range c.Groups {
		servers := make([]string, 0, len(c.Groups[k]))
		servers = append(servers, c.Groups[k]...)
		config.Groups[k] = servers
	}
	return config
}

type Args interface {
	Equal(o Args) bool
	String() string
}

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
	ErrUnknownOp   = "ErrUnknownOperation"
)

type Err string

type JoinArgs struct {
	Servers   map[int][]string // new GID -> servers mappings
	ClientId  int64
	RequestId int64
}

func (a JoinArgs) Equal(o Args) bool {
	b, ok := o.(JoinArgs)
	if !ok {
		return false
	}
	if len(a.Servers) != len(b.Servers) {
		return false
	}
	for gid, aservers := range a.Servers {
		if bservers, ok := b.Servers[gid]; !ok {
			return false
		} else {
			if len(aservers) != len(bservers) {
				return false
			}
			for i := range aservers {
				if aservers[i] != bservers[i] {
					return false
				}
			}
		}
	}
	return true
}

func (a JoinArgs) String() string {
	return fmt.Sprintf("JoinArgs{Servers: %v, ClientId: %d, RequestId: %d}", a.Servers, a.ClientId, a.RequestId)
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs      []int
	ClientId  int64
	RequestId int64
}

func (a LeaveArgs) Equal(o Args) bool {
	b, ok := o.(LeaveArgs)
	if !ok {
		return false
	}
	if a.ClientId != b.ClientId || a.RequestId != b.RequestId {
		return false
	}
	if len(a.GIDs) != len(b.GIDs) {
		return false
	}
	for i := range a.GIDs {
		if a.GIDs[i] != b.GIDs[i] {
			return false
		}
	}
	return true
}

func (a LeaveArgs) String() string {
	return fmt.Sprintf("LeaveArgs{GIDs: %v, ClientId: %d, RequestId: %d}", a.GIDs, a.ClientId, a.RequestId)
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard     int
	GID       int
	ClientId  int64
	RequestId int64
}

func (a MoveArgs) Equal(o Args) bool {
	b, ok := o.(MoveArgs)
	if !ok {
		return false
	}
	return a == b
}

func (a MoveArgs) String() string {
	return fmt.Sprintf("MoveArgs{Shard: %d, GID: %d, ClientId: %d, RequestId: %d}", a.Shard, a.GID, a.ClientId, a.RequestId)
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num       int // desired config number
	ClientId  int64
	RequestId int64
}

func (a QueryArgs) Equal(o Args) bool {
	b, ok := o.(QueryArgs)
	if !ok {
		return false
	}
	return a == b
}

func (a QueryArgs) String() string {
	return fmt.Sprintf("QueryArgs{Num: %d, ClientId: %d, RequestId: %d}", a.Num, a.ClientId, a.RequestId)
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
