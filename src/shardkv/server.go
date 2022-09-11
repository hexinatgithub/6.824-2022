package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

const debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if debug {
		log.Printf(format, a...)
	}
	return
}

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
	dGet          logTopic = "Get"
	dPutAppend    logTopic = "PutAppend"
	dApply        logTopic = "Apply"
	dSnap         logTopic = "Snap"
	dRestore      logTopic = "Restore"
	dOp           logTopic = "Op"
	dReconfigure  logTopic = "Reconfigure"
	dHandoff      logTopic = "Handoff"
	dMigration    logTopic = "Migration"
	dInit         logTopic = "Init"
	dMergeHistory logTopic = "MergeHistory"
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

type Action int

const (
	ActGet Action = iota
	ActPut
	ActAppend
)

var (
	ats = map[Action]string{
		ActGet:    "Get",
		ActPut:    "Put",
		ActAppend: "Append",
	}
	sta = map[string]Action{
		"Get":    ActGet,
		"Put":    ActPut,
		"Append": ActAppend,
	}
)

func (act Action) String() string {
	return ats[act]
}

func action(op string) Action {
	return sta[op]
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key    string
	Value  string
	Action Action

	ClientId  int64
	RequestId int64
}

func (o Op) String() string {
	if o.Action == ActGet {
		return fmt.Sprintf(`Op{Key: %q, Action: "%s", ClientId: %d, RequestId: %d}`, o.Key, o.Action, o.ClientId, o.RequestId)
	}
	return fmt.Sprintf(`Op{Key: %q, Value: %q, Action: "%s", ClientId: %d, RequestId: %d}`, o.Key, o.Value, o.Action, o.ClientId, o.RequestId)
}

type Reconfigure struct {
	Config shardctrler.Config
}

func (c *Reconfigure) DeepCopy() Reconfigure {
	return Reconfigure{
		Config: c.Config.DeepCopy(),
	}
}

func (c Reconfigure) String() string {
	return fmt.Sprintf(`Reconfigure{Config: %s}`, c.Config)
}

type Handoff struct {
	Dest  int
	Num   int
	Shard []*Shard
}

func (ho *Handoff) DeepCopy() Handoff {
	c := Handoff{Dest: ho.Dest, Num: ho.Num}
	for i := range ho.Shard {
		c.Shard = append(c.Shard, ho.Shard[i].DeepCopy())
	}
	return c
}

func (h Handoff) String() string {
	return fmt.Sprintf(`Handoff{Dest: %d, Num: %d, Shard: %v}`, h.Dest, h.Num, h.Shard)
}

type ShardWithSession struct {
	Shard   *Shard
	Session []Track
}

func (sws *ShardWithSession) DeepCopy() ShardWithSession {
	c := ShardWithSession{
		Shard: sws.Shard.DeepCopy(),
	}
	c.Session = append(c.Session, sws.Session...)
	return c
}

func (sws ShardWithSession) String() string {
	var ss []string
	for i := range sws.Session {
		ss = append(ss, sws.Session[i].String())
	}
	return fmt.Sprintf("ShardWithSession{Shard: %s, Session: [%s]}", sws.Shard.String(), strings.Join(ss, ","))
}

type Migration struct {
	Source           int
	Num              int
	ShardWithSession []ShardWithSession
}

func (mig *Migration) DeepCopy() Migration {
	c := Migration{Source: mig.Source, Num: mig.Num}
	for i := range mig.ShardWithSession {
		c.ShardWithSession = append(c.ShardWithSession, mig.ShardWithSession[i].DeepCopy())
	}
	return c
}

func (c Migration) String() string {
	return fmt.Sprintf(`Migration{Source: %d, Num: %d, ShardWithSession: %v}`, c.Source, c.Num, c.ShardWithSession)
}

type commitWait struct {
	index  int
	term   int
	result chan Result
}

func (c commitWait) String() string {
	return fmt.Sprintf(`commitWait{index: %d, term: %d}`, c.index, c.term)
}

type Result struct {
	Err   Err
	Value string
}

func (r Result) String() string {
	if r.Value == "" {
		return fmt.Sprintf(`result{Err: "%s"}`, r.Err)
	}
	return fmt.Sprintf(`result{Err: "%s", Value: "%s"}`, r.Err, r.Value)
}

type Track struct {
	ShardId   int
	ClientId  int64
	RequestId int64
	Result    Result
}

func (t Track) String() string {
	return fmt.Sprintf("Track{ShardId: %d, ClientId: %d, RequestId: %d, Result: %s}", t.ShardId, t.ClientId, t.RequestId, t.Result)
}

type Session struct {
	// Session map clientId -> Track
	Session map[int64]Track
	// ClientShard map clientId -> shardId
	ClientShard map[int64]int
	// ShardClients map shardId -> set of clientId
	ShardClients map[int]map[int64]struct{}
}

func makeSession() Session {
	return Session{
		Session:      make(map[int64]Track),
		ClientShard:  make(map[int64]int),
		ShardClients: make(map[int]map[int64]struct{}),
	}
}

func (s *Session) get(clientId int64) (Track, bool) {
	trk, ok := s.Session[clientId]
	return trk, ok
}

func (s *Session) set(trk Track) {
	clientId := trk.ClientId

	if sess, ok := s.Session[clientId]; ok && trk.RequestId <= sess.RequestId {
		return
	}

	if _, ok := s.ShardClients[trk.ShardId]; !ok {
		s.ShardClients[trk.ShardId] = make(map[int64]struct{})
	}
	s.Session[clientId] = trk
	s.ShardClients[trk.ShardId][clientId] = struct{}{}

	// delete old shard track if neccessary
	// if there is a client track, there must only
	// have one shard track
	if si, ok := s.ClientShard[clientId]; ok && si != trk.ShardId {
		if _, ok := s.ShardClients[si][clientId]; !ok {
			panic("set")
		}

		delete(s.ShardClients[si], clientId)
		if len(s.ShardClients[si]) == 0 {
			delete(s.ShardClients, si)
		}
	}
	s.ClientShard[clientId] = trk.ShardId
}

func (s *Session) batch(trks []Track) {
	for i := range trks {
		s.set(trks[i])
	}
}

// pop pop client Track that locate at that shard
func (s *Session) pop(shardId int) []Track {
	var result []Track
	for clientId := range s.ShardClients[shardId] {
		if _, ok := s.Session[clientId]; !ok {
			panic("pop")
		}
		if si, ok := s.ClientShard[clientId]; !ok || si != shardId {
			panic("pop")
		}
		result = append(result, s.Session[clientId])
		delete(s.Session, clientId)
		delete(s.ClientShard, clientId)
	}
	delete(s.ShardClients, shardId)
	return result
}

type Shard struct {
	Id      int
	Version int
	State   map[string]string
}

func newShard(id, version int) *Shard {
	return &Shard{
		Id:      id,
		Version: version,
		State:   make(map[string]string),
	}
}

func (s *Shard) get(key string) (string, bool) {
	value, ok := s.State[key]
	return value, ok
}

func (s *Shard) set(key, value string) {
	s.State[key] = value
}

func (s *Shard) append(key, value string) {
	if ss, ok := s.State[key]; ok && strings.Contains(ss, value) {
		s.State[key] += value
		return
	}
	s.State[key] += value
}

func (s *Shard) DeepCopy() *Shard {
	c := Shard{
		Id:      s.Id,
		Version: s.Version,
		State:   make(map[string]string),
	}
	for k := range s.State {
		c.State[k] = s.State[k]
	}
	return &c
}

func (s *Shard) String() string {
	return fmt.Sprintf("Shard<%d:V%d>", s.Id, s.Version)
}

type Movement struct {
	SWS ShardWithSession
}

func (m Movement) DeepCopy() Movement {
	c := Movement{
		SWS: m.SWS.DeepCopy(),
	}
	return c
}

func (m Movement) String() string {
	return fmt.Sprintf("Movement{SWS: %s}", m.SWS)
}

type History struct {
	Group   int
	History [shardctrler.NShards]int
}

func (h History) String() string {
	return fmt.Sprintf("History{Group: %d, History: %v}", h.Group, h.History)
}

// Storage store owned shards, cache more update shard,
// stash contain shard need to migration.
// Storage must maintain one invariant:
// 1. shards and stash shard intersection must be empty,
// shard must only exist in shards or stash, but not both.
type Storage struct {
	// Shards map shardId -> owned Shard
	Shards map[int]*Shard
	// Cache map shardId -> newer config Shard
	Cache map[int]Movement
	// Stash map shardId -> Movement
	Stash map[int]Movement
	// Waitfor is wait for shard to transfer
	Waitfor map[int]struct{}
}

func makeStorage() Storage {
	return Storage{
		Shards:  make(map[int]*Shard),
		Cache:   make(map[int]Movement),
		Stash:   make(map[int]Movement),
		Waitfor: make(map[int]struct{}),
	}
}

func (s *Storage) store(sha *Shard) {
	if s.stored(sha.Id) || s.staged(sha.Id) {
		panic("store")
	}
	s.Shards[sha.Id] = sha
	delete(s.Waitfor, sha.Id)
}

func (s *Storage) stored(si int) bool {
	_, ok := s.Shards[si]
	return ok
}

func (s *Storage) cache(mv Movement) {
	if s.cached(mv.SWS.Shard.Id) || s.staged(mv.SWS.Shard.Id) {
		panic("cache")
	}
	s.Cache[mv.SWS.Shard.Id] = mv
}

func (s *Storage) cached(si int) bool {
	_, ok := s.Cache[si]
	return ok
}

func (s *Storage) stage(mv Movement) {
	if s.stored(mv.SWS.Shard.Id) || s.staged(mv.SWS.Shard.Id) {
		panic("stage")
	}
	s.Stash[mv.SWS.Shard.Id] = mv
}

func (s *Storage) staged(si int) bool {
	_, ok := s.Stash[si]
	return ok
}

func (s *Storage) handoff(si int) {
	delete(s.Stash, si)
}

func (s *Storage) wait(sis ...int) {
	for _, si := range sis {
		s.Waitfor[si] = struct{}{}
	}
}

func (s *Storage) waiting() bool {
	return len(s.Waitfor) != 0
}

func (s *Storage) waiton(si int) bool {
	_, ok := s.Waitfor[si]
	return ok
}

func (s *Storage) transfering() bool {
	return len(s.Stash) != 0
}

func (s *Storage) String() string {
	var sis []int
	var s1 []string
	var s2 []string
	var s3 []string

	for si := range s.Shards {
		sis = append(sis, si)
	}
	sort.Ints(sis)
	for _, si := range sis {
		s1 = append(s1, s.Shards[si].String())
	}

	sis = nil
	for si := range s.Stash {
		sis = append(sis, si)
	}
	sort.Ints(sis)
	for _, si := range sis {
		s2 = append(s2, s.Stash[si].String())
	}

	sis = nil
	for si := range s.Cache {
		sis = append(sis, si)
	}
	sort.Ints(sis)
	for _, si := range sis {
		s3 = append(s3, s.Cache[si].String())
	}

	return fmt.Sprintf("Storage{Shards: [%s], Stash: [%s], Cache: [%s]}", strings.Join(s1, ","), strings.Join(s2, ","), strings.Join(s3, ","))
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mck    *shardctrler.Clerk
	killed int32

	persister *raft.Persister
	wait      map[int]commitWait
	stopCh    chan struct{}

	storage     Storage
	session     Session
	cfg         shardctrler.Config
	lastApplied int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Key:       args.Key,
		Action:    ActGet,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	wait, wrongLeader := kv.proposeOp(op)
	if wrongLeader {
		reply.Err = ErrWrongLeader
		return
	}

	result := <-wait.result
	reply.Value = result.Value
	reply.Err = result.Err
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		Action:    action(args.Op),
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	wait, wrongLeader := kv.proposeOp(op)
	if wrongLeader {
		reply.Err = ErrWrongLeader
		return
	}

	result := <-wait.result
	reply.Err = result.Err
}

func (kv *ShardKV) Migration(args *MigrationArgs, reply *MigrationReply) {
	ca := args.DeepCopy()
	mig := Migration{
		Source:           ca.Source,
		Num:              ca.Num,
		ShardWithSession: ca.ShardWithSession,
	}
	wait, wrongLeader := kv.proposeMigration(mig)
	if wrongLeader {
		reply.Err = ErrWrongLeader
		return
	}

	result := <-wait.result
	reply.Err = result.Err
}

func (kv *ShardKV) proposeOp(op Op) (*commitWait, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.Killed() {
		return nil, true
	}

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		return nil, true
	}
	wait := commitWait{
		index:  index,
		term:   term,
		result: make(chan Result, 1),
	}
	kv.wait[index] = wait
	return &wait, false
}

func (kv *ShardKV) proposeReconfigure(rc Reconfigure) {
	if kv.Killed() {
		return
	}
	kv.rf.Start(rc)
}

func (kv *ShardKV) proposeHandoff(hf Handoff) {
	if kv.Killed() {
		return
	}
	kv.rf.Start(hf)
}

func (kv *ShardKV) proposeMigration(mg Migration) (*commitWait, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.Killed() {
		return nil, true
	}

	index, term, isLeader := kv.rf.Start(mg)
	if !isLeader {
		return nil, true
	}
	wait := commitWait{
		index:  index,
		term:   term,
		result: make(chan Result, 1),
	}
	kv.wait[index] = wait
	return &wait, false
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.killed, 1)
	close(kv.stopCh)
}

func (kv *ShardKV) Killed() bool {
	z := atomic.LoadInt32(&kv.killed)
	return z == 1
}

func (kv *ShardKV) apply() {
	for {
		select {
		case applyMsg := <-kv.applyCh:
			if applyMsg.CommandValid {
				kv.applyCommand(&applyMsg)
			} else {
				kv.applySnapshot(&applyMsg)
			}
		case <-kv.stopCh:
			return
		}
	}
}

func (kv *ShardKV) gc() {
	const interval = time.Millisecond * 500
	for {
		select {
		case <-time.After(interval):
			func() {
				kv.mu.Lock()
				defer kv.mu.Unlock()
				var di []int
				term, _ := kv.rf.GetState()

				// clear and respond to commitWait if
				// server is not a Leader
				for i := range kv.wait {
					if kv.wait[i].term != term {
						kv.wait[i].result <- Result{Err: ErrWrongLeader}
						close(kv.wait[i].result)
						di = append(di, i)
					}
				}
				for i := range di {
					delete(kv.wait, di[i])
				}
				kv.trySnap()
			}()
		case <-kv.stopCh:
			kv.mu.Lock()
			for i := range kv.wait {
				close(kv.wait[i].result)
			}
			kv.wait = nil
			kv.mu.Unlock()
			return
		}
	}
}

func (kv *ShardKV) sync() {
	_, isLeader := kv.rf.GetState()
	if isLeader {
		kv.mu.Lock()
		num := kv.cfg.Num
		kv.mu.Unlock()

		cfg := kv.mck.Query(num + 1)
		if cfg.Num == num+1 {
			kv.proposeReconfigure(Reconfigure{cfg})
		}
	}
}

func (kv *ShardKV) watch() {
	const interval = 100 * time.Millisecond
	kv.sync()

	for {
		select {
		case <-time.After(interval):
			kv.sync()
		case <-kv.stopCh:
			return
		}
	}
}

func (kv *ShardKV) deliver() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}

	buffer := make(map[int]*MigrationArgs)

	// merge multi Movement into one request
	for si := range kv.storage.Stash {
		mv := kv.storage.Stash[si].DeepCopy()
		dest := kv.cfg.Shards[si]

		if _, ok := buffer[dest]; !ok {
			buffer[dest] = &MigrationArgs{
				Source:           kv.gid,
				Dest:             dest,
				Num:              kv.cfg.Num,
				ShardWithSession: []ShardWithSession{mv.SWS},
			}
		} else {
			buffer[dest].ShardWithSession = append(buffer[dest].ShardWithSession, mv.SWS)
		}
	}

	for gid, args := range buffer {
		if servers, ok := kv.cfg.Groups[gid]; ok {
			go kv.handoff(servers, gid, args)
			Debug(dHandoff, "%s send %s to %d", kv, args, gid)
		}
	}
}

func stripSession(sws []ShardWithSession) []*Shard {
	var res []*Shard
	for i := range sws {
		res = append(res, sws[i].Shard)
	}
	return res
}

func (kv *ShardKV) handoff(servers []string, gid int, args *MigrationArgs) {
	reply := &MigrationReply{}
	if kv.sendMigration(servers, args, reply) {
		if reply.Err != OK {
			return
		}

		kv.mu.Lock()
		num := kv.cfg.Num
		transfering := kv.storage.transfering()
		kv.mu.Unlock()

		if num != args.Num || !transfering {
			return
		}

		ho := Handoff{
			Dest:  args.Dest,
			Num:   args.Num,
			Shard: stripSession(args.ShardWithSession),
		}
		kv.proposeHandoff(ho)
	}
}

func (kv *ShardKV) sendMigration(servers []string, args *MigrationArgs, reply *MigrationReply) bool {
	// try each server for the group.
	for i := 0; i < len(servers); i++ {
		srv := kv.make_end(servers[i])
		ok := srv.Call("ShardKV.Migration", args, reply)
		if ok && reply.Err == OK {
			return true
		}

		if ok && reply.Err == ErrWrongGroup {
			break
		}
	}
	return false
}

func (kv *ShardKV) migrate() {
	const interval = 300 * time.Millisecond
	for {
		select {
		case <-time.After(interval):
			kv.deliver()
		case <-kv.stopCh:
			return
		}
	}
}

func (kv *ShardKV) owned(si int) bool {
	return kv.cfg.Shards[si] == kv.gid
}

func (kv *ShardKV) applyOp(index int, op Op) {
	si := key2shard(op.Key)

	if !kv.owned(si) {
		Debug(dApply, "%s not own Key %q", kv, op.Key)
		kv.reply(index, op, Result{Err: ErrWrongGroup})
		return
	} else if kv.storage.waiton(si) {
		Debug(dApply, "%s wait for Shard<%d>", kv, si)
		kv.reply(index, op, Result{Err: ErrWrongGroup})
		return
	}

	trk, trkOk := kv.session.get(op.ClientId)
	if trkOk && trk.RequestId == op.RequestId {
		kv.reply(index, op, trk.Result)
		return
	}

	var res Result
	switch op.Action {
	case ActGet:
		value, ok := kv.storage.Shards[si].get(op.Key)
		if ok {
			res.Err = OK
			res.Value = value
		} else {
			res.Err = ErrNoKey
		}
	case ActPut:
		kv.storage.Shards[si].set(op.Key, op.Value)
		res.Err = OK
	case ActAppend:
		kv.storage.Shards[si].append(op.Key, op.Value)
		res.Err = OK
	}
	Debug(dApply, "%s apply %d Log, command is %s", kv, index, op)
	trk = Track{
		ShardId:   si,
		ClientId:  op.ClientId,
		RequestId: op.RequestId,
		Result:    res,
	}
	kv.session.set(trk)
	Debug(dApply, "%s set client[%d] session to %s", kv, trk.ClientId, trk)

	kv.checkAndReply(index, op, res)
}

func (kv *ShardKV) initial(cfg shardctrler.Config) {
	Debug(dInit, "%s first join cluster", kv)
	for si := range cfg.Shards {
		if cfg.Shards[si] == kv.gid {
			kv.storage.store(newShard(si, 1))
			Debug(dInit, "%s create init %s", kv, kv.storage.Shards[si])
		}
	}
}

func (kv *ShardKV) applyReconfigure(rc Reconfigure) {
	if kv.cfg.Num+1 != rc.Config.Num {
		// do nothing
	} else if kv.cfg.Num == 0 {
		kv.initial(rc.Config)
		kv.cfg = rc.Config
	} else if !kv.storage.transfering() && !kv.storage.waiting() {
		// move shard from Shards to Stash
		var toStash []int

		for si := range kv.storage.Shards {
			if gid := rc.Config.Shards[si]; gid != kv.gid {
				toStash = append(toStash, si)
			}
		}

		for _, si := range toStash {
			mv := Movement{
				SWS: ShardWithSession{
					Shard:   kv.storage.Shards[si],
					Session: kv.session.pop(si),
				},
			}
			mv.SWS.Shard.Version = kv.cfg.Num
			delete(kv.storage.Shards, si)
			kv.stage(mv)
		}

		waitfor := kv.waitfor(rc.Config)
		kv.storage.wait(waitfor...)

		// move shard from Cache to Shards
		for _, si := range waitfor {
			if kv.storage.cached(si) {
				kv.accept(si)
			}
		}

		Debug(dReconfigure, "%s detect config change, update config from %s to %s", kv, kv.cfg, rc.Config)
		kv.cfg = rc.Config
	}
}

func (kv *ShardKV) waitfor(cfg shardctrler.Config) []int {
	var res []int
	for si, gid := range cfg.Shards {
		if gid == kv.gid && !kv.storage.stored(si) {
			res = append(res, si)
		}
	}
	return res
}

func (kv *ShardKV) applyHandoff(ho Handoff) {
	if ho.Num != kv.cfg.Num {
		Debug(dHandoff, "%s config is not same, reject %s", kv, ho)
		return
	}

	if !kv.storage.transfering() {
		return
	}

	for i := range ho.Shard {
		if kv.storage.staged(ho.Shard[i].Id) {
			Debug(dHandoff, "%s handoff %s to %d", kv, ho.Shard[i], ho.Dest)
			kv.storage.handoff(ho.Shard[i].Id)
		}
	}
}

func (kv *ShardKV) applyMigration(index int, mg Migration) {
	if mg.Num < kv.cfg.Num {
		kv.reply(index, mg, Result{Err: OK})
		return
	}

	if mg.Num > kv.cfg.Num {
		for _, sws := range mg.ShardWithSession {
			kv.cache(sws)
		}
		kv.checkAndReply(index, mg, Result{Err: OK})
		return
	}

	if !kv.storage.waiting() {
		Debug(dMigration, "%s no more shards need to receive, reject %s", kv, mg)
		kv.reply(index, mg, Result{Err: OK})
	} else {
		for _, sws := range mg.ShardWithSession {
			kv.merge(sws)
		}
		kv.checkAndReply(index, mg, Result{Err: OK})
	}
}

func (kv *ShardKV) reply(index int, command interface{}, res Result) {
	if wait, waitOk := kv.wait[index]; waitOk {
		wait.result <- res
		close(wait.result)
		delete(kv.wait, index)
		Debug(dApply, "%s reply %s with %s", kv, command, res)
	}
}

func (kv *ShardKV) checkAndReply(index int, command interface{}, res Result) {
	wait, waitOk := kv.wait[index]
	if !waitOk {
		return
	}

	if term, _ := kv.rf.GetState(); wait.term != term {
		wait.result <- Result{Err: ErrWrongLeader}
		close(wait.result)
		delete(kv.wait, index)
		Debug(dApply, "%s %s command is not commited, reply %s", kv, wait, ErrWrongLeader)
		return
	}
	wait.result <- res
	Debug(dApply, "%s reply %s with %s", kv, command, res)
	close(wait.result)
	delete(kv.wait, index)
}

func (kv *ShardKV) cache(sws ShardWithSession) {
	if kv.storage.cached(sws.Shard.Id) {
		version := kv.storage.Cache[sws.Shard.Id].SWS.Shard.Version
		switch version {
		case sws.Shard.Version:
			return
		default:
			panic("cache")
		}
	}
	Debug(dMigration, "%s cache %s", kv, sws)
	kv.storage.handoff(sws.Shard.Id)
	kv.storage.cache(Movement{sws})
}

func (kv *ShardKV) merge(sws ShardWithSession) {
	if sws.Shard.Version+1 != kv.cfg.Num {
		panic("merge")
	}

	if kv.storage.waiton(sws.Shard.Id) && !kv.storage.stored(sws.Shard.Id) {
		Debug(dMigration, "%s merge %s", kv, sws)
		sws.Shard.Version = kv.cfg.Num
		kv.storage.store(sws.Shard)
		kv.session.batch(sws.Session)
		return
	}
	Debug(dMigration, "%s already merge %s, reject it", kv, sws)
}

// accept accept Shard and Session from cache
func (kv *ShardKV) accept(si int) {
	if !kv.storage.waiton(si) || !kv.storage.cached(si) {
		panic("accept")
	}

	mv := kv.storage.Cache[si]
	delete(kv.storage.Cache, si)
	if mv.SWS.Shard.Version != kv.cfg.Num {
		panic("accept")
	}

	kv.storage.store(mv.SWS.Shard)
	kv.session.batch(mv.SWS.Session)
	Debug(dMigration, "%s accept cached %s", kv, mv)
}

func (kv *ShardKV) stage(mv Movement) {
	kv.storage.stage(mv)
	Debug(dMigration, "%s stage %s", kv, mv)
}

func (kv *ShardKV) applyCommand(applyMsg *raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch c := applyMsg.Command.(type) {
	case Reconfigure:
		kv.applyReconfigure(c.DeepCopy())
	case Handoff:
		kv.applyHandoff(c.DeepCopy())
	case Migration:
		kv.applyMigration(applyMsg.CommandIndex, c.DeepCopy())
	case Op:
		kv.applyOp(applyMsg.CommandIndex, c)
	default:
		panic("unknown command")
	}

	kv.lastApplied = applyMsg.CommandIndex
	kv.trySnap()
}

func (kv *ShardKV) applySnapshot(applyMsg *raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.restore(applyMsg.Snapshot)
	Debug(dSnap, "%s apply snapshot at %d", kv, kv.lastApplied)
}

func (kv *ShardKV) trySnap() {
	if kv.maxraftstate != -1 {
		left := kv.maxraftstate - kv.persister.RaftStateSize()
		thre := int(float64(kv.maxraftstate) * 0.1)
		if left <= thre {
			kv.snapshot()
		}
	}
}

func (kv *ShardKV) snapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.storage)
	e.Encode(kv.session)
	e.Encode(kv.lastApplied)
	e.Encode(kv.cfg)
	data := w.Bytes()
	kv.rf.Snapshot(kv.lastApplied, data)
	Debug(dSnap, "%s take a snapshot at %d", kv, kv.lastApplied)
}

func (kv *ShardKV) restore(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	storage := makeStorage()
	session := makeSession()
	lastApplied := 0
	cfg := shardctrler.Config{}
	if d.Decode(&storage) != nil ||
		d.Decode(&session) != nil ||
		d.Decode(&lastApplied) != nil ||
		d.Decode(&cfg) != nil {
		panic("readPersist")
	} else {
		kv.storage = storage
		kv.session = session
		kv.lastApplied = lastApplied
		kv.cfg = cfg
	}
	Debug(dRestore, "%s restore from image, lastApplied %d", kv, kv.lastApplied)
}

func (kv *ShardKV) String() string {
	return fmt.Sprintf("S-%d-%d<Num: %d>", kv.gid, kv.me, kv.cfg.Num)
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(Reconfigure{})
	labgob.Register(Migration{})
	labgob.Register(Handoff{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// Your initialization code here.
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.persister = persister
	kv.wait = make(map[int]commitWait)
	kv.stopCh = make(chan struct{})
	kv.storage = makeStorage()
	kv.session = makeSession()
	kv.restore(persister.ReadSnapshot())
	go kv.apply()
	go kv.gc()
	go kv.watch()
	go kv.migrate()

	return kv
}
