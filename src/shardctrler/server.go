package shardctrler

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const debug = false

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

type logTopic string

const (
	dJoin    logTopic = "Join"
	dLeave   logTopic = "Leave"
	dMove    logTopic = "Move"
	dQuery   logTopic = "Query"
	dApply   logTopic = "Apply"
	dSnap    logTopic = "Snap"
	dRestore logTopic = "Restore"
	dShard   logTopic = "Shard"
)

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic logTopic, format string, a ...interface{}) {
	if debug {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

type Track struct {
	RequestId int64
	Result    Result
}

// maxraftstate exceed this size will cause ShardCtrler snapshot
const maxraftstate = 1024

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	persister *raft.Persister
	wait      map[int]commitWait
	stopCh    chan struct{}
	killed    int32

	configs     []Config // indexed by config num
	session     map[int64]Track
	lastApplied int
}

type Action int

const (
	ActJoin Action = iota
	ActLeave
	ActMove
	ActQuery
)

var (
	atos = map[Action]string{
		ActJoin:  "Join",
		ActLeave: "Leave",
		ActMove:  "Move",
		ActQuery: "Query",
	}
)

func (a Action) String() string {
	return atos[a]
}

type Op struct {
	// Your data here.
	ClientId  int64
	RequestId int64
	Action    Action
	Args      Args
}

func (op Op) equal(o Op) bool {
	if op.ClientId != o.ClientId ||
		op.RequestId != o.RequestId ||
		op.Action != o.Action {
		return false
	}
	if !op.Args.Equal(o.Args) {
		return false
	}
	return true
}

func (op Op) String() string {
	return fmt.Sprintf(`Op{Action: "%s", Args: %s}`, op.Action, op.Args)
}

type Result struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

func (r Result) String() string {
	return fmt.Sprintf(`Result{WrongLeader: %t, Err: "%s", Config: %s}`,
		r.WrongLeader, r.Err, r.Config)
}

type commitWait struct {
	term   int
	op     Op
	result chan Result
}

func (w commitWait) String() string {
	return fmt.Sprintf("commitWait{term: %d, op: %s}", w.term, w.op)
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Action:    ActJoin,
		Args:      *args,
	}
	wait, wrongLeader := sc.commit(op)
	if wrongLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	Debug(dJoin, "%s handle %s, start commit, wait for result", sc, op)
	result := <-wait.result
	reply.Err = result.Err
	reply.WrongLeader = result.WrongLeader
	Debug(dJoin, "%s proccess %s complete", sc, op)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Action:    ActLeave,
		Args:      *args,
	}
	wait, wrongLeader := sc.commit(op)
	if wrongLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	Debug(dJoin, "%s handle %s, start commit, wait for result", sc, op)
	result := <-wait.result
	reply.Err = result.Err
	reply.WrongLeader = result.WrongLeader
	Debug(dJoin, "%s proccess %s complete", sc, op)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Action:    ActMove,
		Args:      *args,
	}
	wait, wrongLeader := sc.commit(op)
	if wrongLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	Debug(dJoin, "%s handle %s, start commit, wait for result", sc, op)
	result := <-wait.result
	reply.Err = result.Err
	reply.WrongLeader = result.WrongLeader
	Debug(dJoin, "%s proccess %s complete", sc, op)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Action:    ActQuery,
		Args:      *args,
	}
	wait, wrongLeader := sc.commit(op)
	if wrongLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	Debug(dJoin, "%s handle %s, start commit, wait for result", sc, op)
	result := <-wait.result
	reply.Err = result.Err
	reply.WrongLeader = result.WrongLeader
	if !result.WrongLeader {
		reply.Config = result.Config
	}
	Debug(dJoin, "%s proccess %s complete", sc, op)
}

func (sc *ShardCtrler) commit(op Op) (*commitWait, bool) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.Killed() {
		return nil, true
	}

	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		Debug(dJoin, "%s is not a Leader", sc)
		return nil, true
	}
	wait := commitWait{
		term:   term,
		op:     op,
		result: make(chan Result, 1),
	}
	sc.wait[index] = wait
	sc.trySnap()
	return &wait, false
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.killed, 1)
	close(sc.stopCh)
}

func (sc *ShardCtrler) Killed() bool {
	z := atomic.LoadInt32(&sc.killed)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) apply() {
	for {
		select {
		case applyMsg := <-sc.applyCh:
			if applyMsg.CommandValid {
				sc.applyCommand(&applyMsg)
			} else {
				sc.applySnapshot(&applyMsg)
			}
		case <-sc.stopCh:
			return
		}
	}
}

type sharder []struct {
	gid    int
	shards []int
}

func (s sharder) Len() int {
	return len(s)
}

func (s sharder) Less(i, j int) bool {
	return len(s[i].shards) < len(s[j].shards)
}

func (s sharder) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s sharder) balance(c *Config) {
	nGroups := len(s)
	avg := NShards / nGroups
	remain := NShards % nGroups
	more1 := nGroups - remain
	basis := func(i int) int {
		if remain == 0 {
			return avg
		}
		if i >= more1 {
			return avg + 1
		}
		return avg
	}

	i, j := 0, nGroups-1
	sort.Sort(s)

	for i < j {
		up, down := basis(i), basis(j)

		for len(s[i].shards) < up && len(s[j].shards) > down {
			li := len(s[j].shards) - 1
			s[i].shards = append(s[i].shards, s[j].shards[li])
			s[j].shards = s[j].shards[:li]
		}

		if len(s[i].shards) == up {
			i++
		}
		if len(s[j].shards) == down {
			j--
		}
	}

	for i := range s {
		assigned := s[i]
		for j := range assigned.shards {
			shard := assigned.shards[j]
			c.Shards[shard] = assigned.gid
		}
	}
}

func loadBalance(conf *Config) {
	nGroups := len(conf.Groups)

	if nGroups == 0 {
		conf.Shards = [NShards]int{}
		return
	}

	gids := make([]int, nGroups)
	s := make(sharder, nGroups)
	indexer := make(map[int]int, nGroups)

	{
		i := 0
		for gid := range conf.Groups {
			gids[i] = gid
			i++
		}
		sort.Ints(gids)
	}

	for i, gid := range gids {
		s[i].gid = gid
		indexer[gid] = i
	}

	for shard, gid := range conf.Shards {
		i, ok := indexer[gid]
		if !ok {
			s[len(s)-1].shards = append(s[len(s)-1].shards, shard)
		} else {
			s[i].shards = append(s[i].shards, shard)
		}
	}
	s.balance(conf)
}

func (sc *ShardCtrler) reshard(c *Config) {
	loadBalance(c)
	Debug(dShard, "%s reshard servers", sc)
	sc.appendConfig(c)
}

func (sc *ShardCtrler) appendConfig(c *Config) {
	c.Num = sc.configs[len(sc.configs)-1].Num + 1
	sc.configs = append(sc.configs, *c)
	Debug(dShard, "%s append new config: %s", sc, c)
}

type configWrapper struct {
	config Config
}

func (w *configWrapper) join(servers map[int][]string) {
	for gid := range servers {
		w.config.Groups[gid] = servers[gid]
	}
}

func (w *configWrapper) leave(gids []int) {
	for i := range gids {
		delete(w.config.Groups, gids[i])
	}
}

func (w *configWrapper) move(shard, gid int) {
	w.config.Shards[shard] = gid
}

func (w *configWrapper) result() *Config {
	return &w.config
}

// deep copy config and return a configWrapper
func wrapper(conf *Config) *configWrapper {
	return &configWrapper{
		config: conf.DeepCopy(),
	}
}

func (sc *ShardCtrler) applyCommand(applyMsg *raft.ApplyMsg) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if applyMsg.CommandIndex <= sc.lastApplied {
		return
	}

	op := applyMsg.Command.(Op)
	wait, waitOk := sc.wait[applyMsg.CommandIndex]
	trk, trkOk := sc.session[op.ClientId]

	if trkOk && trk.RequestId == op.RequestId {
		if waitOk {
			wait.result <- trk.Result
			Debug(dApply, "%s wait request is duplicate, send saved %s to %s", sc, trk.Result, wait)
			close(wait.result)
			delete(sc.wait, applyMsg.CommandIndex)
		} else {
			Debug(dApply, "%s %s is duplicated", sc, op)
		}
		return
	}

	var res Result
	switch args := op.Args.(type) {
	case JoinArgs:
		wp := sc.getConfigWrapper(-1)
		wp.join(args.Servers)
		sc.reshard(wp.result())
		res.WrongLeader = false
		res.Err = OK
	case LeaveArgs:
		wp := sc.getConfigWrapper(-1)
		wp.leave(args.GIDs)
		sc.reshard(wp.result())
		res.WrongLeader = false
		res.Err = OK
	case MoveArgs:
		wp := sc.getConfigWrapper(-1)
		wp.move(args.Shard, args.GID)
		sc.appendConfig(wp.result())
		res.WrongLeader = false
		res.Err = OK
	case QueryArgs:
		res.WrongLeader = false
		res.Err = OK
		res.Config = sc.getConfig(args.Num).DeepCopy()
	}

	sc.lastApplied = applyMsg.CommandIndex
	Debug(dApply, "%s apply %d Log %s", sc, applyMsg.CommandIndex, op)
	sc.session[op.ClientId] = Track{
		RequestId: op.RequestId,
		Result:    res,
	}
	Debug(dApply, "%s set client[%d] session's requestId to %d", sc, op.ClientId, op.RequestId)

	if !waitOk {
		// do nothing
	} else if term, _ := sc.rf.GetState(); !wait.op.equal(op) || wait.term != term {
		res = Result{WrongLeader: true, Err: ErrWrongLeader}
		for i := range sc.wait {
			sc.wait[i].result <- res
			close(sc.wait[i].result)
		}
		Debug(dApply, "%s lose leadership before apply this Log, reply all wait request %s", sc, res.Err)
		sc.wait = make(map[int]commitWait)
	} else {
		wait.result <- res
		Debug(dApply, "%s send %s to %s", sc, res, wait)
		close(wait.result)
		delete(sc.wait, applyMsg.CommandIndex)
	}
	sc.trySnap()
}

func (sc *ShardCtrler) getConfig(num int) *Config {
	if num == -1 || num >= len(sc.configs) {
		return &sc.configs[len(sc.configs)-1]
	}
	return &sc.configs[num]
}

func (sc *ShardCtrler) getConfigWrapper(num int) *configWrapper {
	c := sc.getConfig(num)
	return wrapper(c)
}

func (sc *ShardCtrler) applySnapshot(applyMsg *raft.ApplyMsg) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.restore(applyMsg.Snapshot)
}

func (sc *ShardCtrler) gc() {
	interval := time.Millisecond * 500
	for {
		select {
		case <-time.After(interval):
			func() {
				sc.mu.Lock()
				defer sc.mu.Unlock()
				var di []int
				term, _ := sc.rf.GetState()
				res := Result{
					WrongLeader: true,
					Err:         ErrWrongLeader,
				}
				for i := range sc.wait {
					if sc.wait[i].term != term {
						sc.wait[i].result <- res
						close(sc.wait[i].result)
						di = append(di, i)
					}
				}
				for i := range di {
					delete(sc.wait, di[i])
				}
				sc.trySnap()
			}()
		case <-sc.stopCh:
			sc.mu.Lock()
			for i := range sc.wait {
				close(sc.wait[i].result)
			}
			sc.wait = make(map[int]commitWait)
			sc.mu.Unlock()
			return
		}
	}
}

func (sc *ShardCtrler) trySnap() {
	if sc.persister.RaftStateSize() > maxraftstate {
		sc.snapshot()
	}
}

func (sc *ShardCtrler) snapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(sc.configs)
	e.Encode(sc.session)
	e.Encode(sc.lastApplied)
	data := w.Bytes()
	sc.rf.Snapshot(sc.lastApplied, data)
	Debug(dSnap, "%s take a snapshot at %d", sc, sc.lastApplied)
}

func (sc *ShardCtrler) restore(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&sc.configs) != nil ||
		d.Decode(&sc.session) != nil ||
		d.Decode(&sc.lastApplied) != nil {
		panic("readPersist")
	}
	Debug(dRestore, "%s restore from image, lastApplied %d", sc, sc.lastApplied)
}

func (sc *ShardCtrler) String() string {
	return fmt.Sprintf("S%d", sc.me)
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.persister = persister
	sc.wait = make(map[int]commitWait)
	sc.stopCh = make(chan struct{})
	sc.session = make(map[int64]Track)
	sc.restore(persister.ReadSnapshot())
	go sc.apply()
	go sc.gc()

	return sc
}
