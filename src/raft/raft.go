package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type status int

const (
	Leader status = iota
	Candidate
	Follower
)

var sm = map[status]string{
	0: "Leader",
	1: "Candidate",
	2: "Follower",
}

func (s status) String() string {
	return sm[s]
}

type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

type Log struct {
	Entries           []Entry
	LastIncludedIndex int
	LastIncludedTerm  int
}

func (l *Log) append(entry Entry) int {
	entry.Index = l.nextIndex()
	l.Entries = append(l.Entries, entry)
	return entry.Index
}

func (l *Log) replace(start int, entries ...Entry) {
	if start <= l.LastIncludedIndex {
		panic("replace")
	}

	i := l.calibrations(start)
	lastIndex := l.last().Index
	for j := 0; j < len(entries); j++ {
		if start <= lastIndex {
			l.Entries[i] = entries[j]
			i++
			start++
		} else {
			l.Entries = append(l.Entries, entries[j])
		}
	}
}

func (l *Log) nextIndex() int {
	if l.LastIncludedIndex != None {
		if len(l.Entries) == 0 {
			return l.LastIncludedIndex + 1
		}
		return l.last().Index + 1
	}
	return len(l.Entries)
}

func (l *Log) slice(start int) []Entry {
	if start <= l.LastIncludedIndex {
		panic("slice last")
	}
	if start > l.nextIndex() {
		panic("slice next")
	}
	i := l.calibrations(start)
	src := l.Entries[i:]
	dst := make([]Entry, len(src))
	copy(dst, src)
	return dst
}

func (l *Log) firstIndexWithSameTerm(back int) int {
	i, j := back, back-1
	for i > l.LastIncludedIndex {
		if l.get(i).Term != l.get(j).Term {
			return i
		}
		i--
		j--
	}
	return i
}

func (l *Log) lastIndexWithSameTerm(start int) int {
	lastIndex := l.last().Index
	i, j := start, start+1
	for i < lastIndex {
		if l.get(i).Term != l.get(j).Term {
			break
		}
		i++
		j++
	}
	return i
}

var (
	None         = -1
	defaultEntry = &Entry{
		Index: None,
		Term:  None,
	}
	empty *Entry = nil
)

func (l *Log) get(i int) *Entry {
	if i == None {
		return defaultEntry
	} else if i < l.LastIncludedIndex {
		panic("get last")
	} else if i >= l.nextIndex() {
		return empty
	}

	i = l.calibrations(i)
	if i == None {
		return &Entry{
			Index: l.LastIncludedIndex,
			Term:  l.LastIncludedTerm,
		}
	}
	result := l.Entries[i]
	return &result
}

func (l *Log) truncate(i int) {
	if i <= l.LastIncludedIndex {
		panic("truncate last")
	}

	next := l.nextIndex()
	if i > next {
		panic("truncate next")
	}

	i = l.calibrations(i)
	l.Entries = l.Entries[:i]
}

func (l *Log) last() *Entry {
	if len(l.Entries) == 0 {
		return l.get(l.LastIncludedIndex)
	}
	result := l.Entries[len(l.Entries)-1]
	return &result
}

func (l *Log) moreUpToDate(index, term int) bool {
	last := l.last()
	if last.Term != term {
		return last.Term > term
	}
	return last.Index > index
}

func (l *Log) calibrations(i int) int {
	if i < l.LastIncludedIndex {
		panic("calibrations")
	}
	if i == l.LastIncludedIndex {
		return None
	}
	if l.LastIncludedIndex != None {
		i = i - l.LastIncludedIndex - 1
	}
	return i
}

func (l *Log) trim(i int) {
	lastInclude := l.get(i)
	l.Entries = l.slice(i + 1)
	l.LastIncludedIndex = lastInclude.Index
	l.LastIncludedTerm = lastInclude.Term
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	term int
	vote int
	log  Log

	commitIndex  int
	lastApplied  int
	status       status
	knock        time.Time
	progress     *progress
	applyCh      chan<- ApplyMsg
	applyCond    *sync.Cond
	isLeaderCond *sync.Cond
	busy         bool
	immedCh      chan struct{}
	image        []byte
}

type progress struct {
	poll       []bool
	nextIndex  []int
	matchIndex []int
}

func (p *progress) win() bool {
	majority := len(p.poll)/2 + 1
	count := 0
	for i := range p.poll {
		if p.poll[i] {
			count++
			if count >= majority {
				return true
			}
		}
	}
	return false
}

func (p *progress) commitIndex() int {
	if len(p.matchIndex) == 1 {
		return p.matchIndex[0]
	}
	tmp := make([]int, len(p.matchIndex))
	copy(tmp, p.matchIndex)
	sort.Ints(tmp)
	return tmp[len(tmp)/2]
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.term
	isleader = rf.status == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.vote)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	Debug(dPersist, "%s save persistent state to stable storage", rf)
}

func (rf *Raft) persistStateAndSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.vote)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
	Debug(dPersist, "%s save persistent state and snapshot to stable storage", rf)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&rf.term) != nil ||
		d.Decode(&rf.vote) != nil ||
		d.Decode(&rf.log) != nil {
		panic("readPersist")
	}
	rf.commitIndex = rf.log.LastIncludedIndex
	rf.lastApplied = rf.log.LastIncludedIndex
	Debug(dSnap, "%s restore persist state, vote for %d, term is %d, lastIncludedIndex is %d, lastIncludedTerm is %d", rf, rf.vote, rf.term, rf.log.LastIncludedIndex, rf.log.LastIncludedTerm)
}

func (rf *Raft) restoreSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	rf.image = data
	Debug(dSnap, "%s restore snapshot from image", rf)
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.log.trim(index - 1)
	rf.lastApplied = max(rf.lastApplied, index-1)
	rf.commitIndex = max(rf.commitIndex, index-1)
	rf.persistStateAndSnapshot(snapshot)
	Debug(dSnap, "%s snapshot at %d", rf, index-1)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XIndex  int
	XTerm   int
	XLen    int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	persist := false
	defer func() {
		if persist {
			rf.persist()
		}
	}()

	if args.Term < rf.term {
		Debug(dDrop, "%s RequestVote args's Term %d is old, current term is %d", rf, args.Term, rf.term)
		reply.Term = rf.term
		reply.VoteGranted = false
		return
	} else if args.Term > rf.term {
		Debug(dTerm, "%s RequestVote args's Term %d is new, current term is %d, change to %d", rf, args.Term, rf.term, args.Term)
		rf.term = args.Term
		rf.vote = -1
		rf.becomeFollower()
		persist = true
	}

	// repeat request vote, grant
	if rf.vote == args.CandidateId {
		Debug(dVote, "%s already vote for %d, grant vote", rf, args.CandidateId, rf.vote)
		reply.Term = rf.term
		reply.VoteGranted = true
		return
	}

	// vote for other peer, just deny
	if rf.vote != -1 {
		Debug(dVote, "%s deny vote for %d, already vote for %d", rf, args.CandidateId, rf.vote)
		reply.Term = rf.term
		reply.VoteGranted = false
		return
	}

	if rf.log.moreUpToDate(args.LastLogIndex, args.LastLogTerm) {
		Debug(dVote, "%s log is more update, deny vote for %d", rf, args.CandidateId)
		reply.Term = rf.term
		reply.VoteGranted = false
		return
	}

	rf.vote = args.CandidateId
	rf.knock = time.Now()
	rf.becomeFollower()
	persist = true
	reply.Term = rf.term
	reply.VoteGranted = true
	Debug(dVote, "%s grant vote for %d, become Follower", rf, args.CandidateId)
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	persist := false
	defer func() {
		if persist {
			rf.persist()
		}
	}()

	if args.Term < rf.term {
		reply.Term = rf.term
		reply.Success = false
		Debug(dDrop, "%s AppendEntries args's Term %d is old, current term is %d, drop message", rf, args.Term, rf.term)
		return
	} else if args.Term > rf.term {
		rf.term = args.Term
		persist = true
		Debug(dTerm, "%s, AppendEntries args's Term is new, current term is %d, change to %d, become Follower", rf, rf.term, args.Term)
	}

	if rf.status != Follower {
		Debug(dInfo, "%s transfer to %s", rf, Follower)
		rf.becomeFollower()
	}
	rf.knock = time.Now()

	entry := rf.log.get(args.PrevLogIndex)
	last := rf.log.last()

	// follower log is short
	if entry == nil {
		reply.Term = rf.term
		reply.Success = false
		reply.XIndex = last.Index + 1
		reply.XTerm = None
		reply.XLen = last.Index + 1
		Debug(dLog, "%s doesn't contain an entry at %d", rf, args.PrevLogIndex)
		return
	}

	// conflict
	if entry.Term != args.PrevLogTerm {
		firstIndex := rf.log.firstIndexWithSameTerm(entry.Index)
		rf.log.truncate(args.PrevLogIndex)
		persist = true
		reply.Term = rf.term
		reply.Success = false
		reply.XIndex = firstIndex
		reply.XTerm = entry.Term
		reply.XLen = args.PrevLogIndex
		Debug(dLog, "%s contain conflict entry at %d, truncate log", rf, args.PrevLogIndex)
		return
	}

	rf.log.replace(args.PrevLogIndex+1, args.Entries...)
	if len(args.Entries) == 0 {
		Debug(dInfo, "%s receive heartbeat", rf)
	} else {
		Debug(dLog, "%s append %d new log entries at %d", rf, len(args.Entries), args.PrevLogIndex+1)
		persist = true
	}
	if args.LeaderCommit > rf.commitIndex {
		tmp := min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		if tmp != rf.commitIndex {
			rf.commitIndex = tmp
			rf.applyCond.Signal()
			Debug(dCommit, "%s update commitIndex to %d", rf, rf.commitIndex)
		}
	}

	reply.Term = rf.term
	reply.Success = true
	return
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.term {
		Debug(dDrop, "%s Leader S%d InstallSnapshot Term %d is new, current Term %d is old, change to %d", rf, args.LeaderId, args.Term, rf.term, args.Term)
		rf.term = args.Term
	} else if args.Term < rf.term {
		reply.Term = rf.term
		Debug(dDrop, "%s Leader S%d InstallSnapshot's Term %d is old, current Term is %d, drop snapshot", rf, args.LeaderId, args.Term, rf.term)
		return
	}
	if args.LastIncludedIndex <= rf.log.LastIncludedIndex {
		reply.Term = rf.term
		Debug(dDrop, "%s Leader S%d InstallSnapshot's snapshot is old, args's LastIncludedIndex is %d, current LastIncludedIndex is %d, drop", rf, args.LeaderId, args.LastIncludedIndex, rf.log.LastIncludedIndex)
		return
	}
	if rf.status != Follower {
		Debug(dInfo, "%s become Follower", rf)
		rf.becomeFollower()
	}

	rf.knock = time.Now()
	if last := rf.log.last(); last.Index < args.LastIncludedIndex {
		rf.log.Entries = make([]Entry, 0)
		rf.log.LastIncludedIndex = args.LastIncludedIndex
		rf.log.LastIncludedTerm = args.LastIncludedTerm
	} else if entry := rf.log.get(args.LastIncludedIndex); entry.Index != args.LastIncludedIndex || entry.Term != args.LastIncludedTerm {
		rf.log.Entries = make([]Entry, 0)
		rf.log.LastIncludedIndex = args.LastIncludedIndex
		rf.log.LastIncludedTerm = args.LastIncludedTerm
	} else {
		rf.log.trim(args.LastIncludedIndex)
	}
	rf.commitIndex = max(args.LastIncludedIndex, rf.commitIndex)
	rf.lastApplied = max(args.LastIncludedIndex, rf.lastApplied)
	rf.image = args.Data
	Debug(dSnap, "%s install snapshot from Leader S%d, update commitIndex to %d, update lastApplied to %d", rf, args.LeaderId, rf.commitIndex, rf.lastApplied)
	rf.persistStateAndSnapshot(args.Data)
	rf.applyCond.Signal()
	reply.Term = rf.term
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.status != Leader {
		Debug(dLog2, "%s is not Leader, drop command", rf)
		return index, term, false
	}

	index = rf.log.append(Entry{
		Term:    rf.term,
		Command: command,
	})
	term = rf.term
	Debug(dInfo, "%s append log at index %d", rf, index)
	rf.persist()
	rf.progress.nextIndex[rf.me] = index + 1
	rf.progress.matchIndex[rf.me] = index

	if !rf.busy {
		rf.busy = true
		rf.immedCh <- struct{}{}
	}
	return index + 1, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.applyCond.Signal()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

const (
	heartBeatInterval = 100 * time.Millisecond
	interval          = 25 * time.Millisecond
)

func randElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(151)+200) * time.Millisecond
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		randomize := randElectionTimeout()
		time.Sleep(interval)

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.status == Leader {
				return
			}

			if time.Now().Sub(rf.knock) < randomize {
				return
			}

			Debug(dInfo, "%s start election, become Candidate", rf)
			rf.becomeCandidate()
			rf.persist()

			last := rf.log.last()
			args := RequestVoteArgs{
				Term:         rf.term,
				CandidateId:  rf.me,
				LastLogIndex: last.Index,
				LastLogTerm:  last.Term,
			}

			for i := range rf.peers {
				if i == rf.me {
					continue
				}

				reply := RequestVoteReply{}
				go rf.handleRequestVote(i, &args, &reply)
			}
		}()
	}
}

func (rf *Raft) handleRequestVote(i int, args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.sendRequestVote(i, args, reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.killed() {
			Debug(dDrop, "%s is killed, drop RequestVote reply", rf)
			return
		} else if reply.Term > rf.term {
			Debug(dTerm, "%s sendRequestVote reply contain a new Term %d, become Follower", rf, reply.Term)
			rf.term = reply.Term
			rf.becomeFollower()
			rf.persist()
			return
		} else if reply.Term < rf.term {
			Debug(dDrop, "%s sendRequestVote reply contain a old Term %d, drop message", rf, reply.Term)
			return
		} else if rf.term != args.Term {
			Debug(dDrop, "%s get a old sendRequestVote reply", rf)
			return
		}

		if rf.status != Candidate {
			Debug(dDrop, "%s status change, not a Candidate, ignore S%d sendRequestVote reply", rf, i)
			return
		}

		if reply.VoteGranted {
			rf.progress.poll[i] = true
			Debug(dVote, "%s S%d grant vote", rf, i)
			if rf.progress.win() {
				Debug(dLeader, "%s collect majority vote, become Leader", rf)
				rf.becomeLeader()
			}
		}
	}
}

func (rf *Raft) heartBeatIfIsLeader() {
	for rf.killed() == false {
		var immedCh chan struct{}
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			for rf.status != Leader {
				rf.isLeaderCond.Wait()
			}

			Debug(dLeader, "%s begin send heartbeat to all peers", rf)
			rf.busy = true
			for i := range rf.peers {
				if i == rf.me {
					continue
				}

				next := rf.progress.nextIndex[i]
				if next <= rf.log.LastIncludedIndex {
					args := InstallSnapshotArgs{
						Term:              rf.term,
						LeaderId:          rf.me,
						LastIncludedIndex: rf.log.LastIncludedIndex,
						LastIncludedTerm:  rf.log.LastIncludedTerm,
						Data:              rf.persister.ReadSnapshot(),
					}
					reply := InstallSnapshotReply{}
					Debug(dInfo, "%s S%d nextIndex %d is too old, sendInstallSnapshot to S%d", rf, i, next, i)
					go rf.handleInstallSnapshot(i, &args, &reply)
				} else {
					entries := rf.log.slice(next)
					preEntry := rf.log.get(next - 1)
					args := AppendEntriesArgs{
						Term:         rf.term,
						LeaderId:     rf.me,
						PrevLogIndex: preEntry.Index,
						PrevLogTerm:  preEntry.Term,
						Entries:      entries,
						LeaderCommit: rf.commitIndex,
					}
					reply := AppendEntriesReply{}
					Debug(dInfo, "%s sendAppendEntries to S%d, nextIndex is %d, entries length is %d", rf, i, next, len(entries))
					go rf.handleAppendEntries(i, &args, &reply)
				}
			}
			immedCh = rf.immedCh
			rf.busy = false
		}()

		select {
		case <-time.After(heartBeatInterval):
		case <-immedCh:
		}
	}
}

func (rf *Raft) handleAppendEntries(i int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.sendAppendEntries(i, args, reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.killed() {
			Debug(dDrop, "%s is killed, drop sendAppendEntries reply", rf)
			return
		}

		if reply.Term > rf.term {
			Debug(dTerm, "%s sendAppendEntries reply contain new Term %d, become Follower", rf, reply.Term)
			rf.term = reply.Term
			rf.becomeFollower()
			rf.persist()
			return
		} else if reply.Term < rf.term {
			Debug(dDrop, "%s sendAppendEntries reply contain old Term %d, current Term is %d, drop message", rf, reply.Term, rf.term)
			return
		} else if rf.term != args.Term {
			Debug(dDrop, "%s get a old sendAppendEntries reply", rf)
			return
		}

		if rf.status != Leader {
			Debug(dLog2, "%s not Leader any more, ignore sendAppendEntries reply", rf)
			return
		}

		if !reply.Success {
			if reply.XTerm == None {
				rf.progress.nextIndex[i] = reply.XIndex
				Debug(dLog, "%s follower S%d log is too short, quick roll back to %d", rf, i, rf.progress.nextIndex[i])
				return
			}

			if reply.XIndex <= rf.log.LastIncludedIndex {
				rf.progress.nextIndex[i] = reply.XIndex
				Debug(dLog, "%s follower S%d is too behind, reply XIndex is %d, Snapshot's lastIncludedIndex is %d", rf, i, reply.XIndex, rf.log.LastIncludedIndex)
				return
			}

			entry := rf.log.get(reply.XIndex)
			if entry.Term != reply.XTerm {
				rf.progress.nextIndex[i] = reply.XIndex
				Debug(dLog, "%s doesn't find Term %d, quick roll back S%d nextIndex to %d", rf, reply.XTerm, i, rf.progress.nextIndex[i])
				return
			}
			rf.progress.nextIndex[i] = rf.log.lastIndexWithSameTerm(reply.XIndex) + 1
			Debug(dLog, "%s contain XTerm %d, set S%d nextIndex to %d", rf, reply.XTerm, i, rf.progress.nextIndex[i])
			return
		}

		if len(args.Entries) == 0 {
			Debug(dLog2, "%s heartbeat reply from S%d, ignore", rf, i)
			return
		}

		rf.progress.nextIndex[i] = args.PrevLogIndex + len(args.Entries) + 1
		rf.progress.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
		Debug(dLog, "%s sendAppendEntries reply success, set S%d nextIndex to %d, matchIndex to %d", rf, i, rf.progress.nextIndex[i], rf.progress.matchIndex[i])

		commitIndex := rf.progress.commitIndex()
		if commitIndex <= rf.log.LastIncludedIndex {
			for i := range rf.progress.matchIndex {
				rf.progress.matchIndex[i] = max(rf.progress.matchIndex[i], rf.log.LastIncludedIndex)
			}
			Debug(dLog, "%s snapshot at index %d, beyond current commit Index: %d, update matchIndex", rf, rf.log.LastIncludedIndex, commitIndex)
			return
		}

		commitLog := rf.log.get(commitIndex)
		if commitLog.Term != rf.term {
			Debug(dCommit, "%s commitIndex %d Term is %d, current Term is %d, index will not commit", rf, rf.commitIndex, commitLog.Term, rf.term)
			return
		}
		if commitIndex > rf.lastApplied && commitIndex != rf.commitIndex {
			rf.commitIndex = commitIndex
			rf.applyCond.Signal()
			Debug(dCommit, "%s update commitIndex to %d", rf, commitIndex)
		}
	}
}

func (rf *Raft) handleInstallSnapshot(i int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if rf.sendInstallSnapshot(i, args, reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.term {
			Debug(dInfo, "%s InstallSnapshot reply's Term %d is new, change to %d", rf, reply.Term, reply.Term)
			rf.term = reply.Term
			rf.becomeFollower()
			return
		} else if reply.Term < rf.term {
			Debug(dInfo, "%s InstallSnapshot reply's Term %d is old, ignore it", rf, reply.Term)
			return
		} else if args.Term != rf.term {
			Debug(dInfo, "%s InstallSnapshot args's Term is %d, current Term is %d, not current Term reply, ignore", rf, args.Term, rf.term)
			return
		}
		if args.LastIncludedIndex != rf.log.LastIncludedIndex {
			Debug(dInfo, "%s InstallSnapshot args's LastIncludedIndex is %d, current LastIncludedIndex is %d, is a old InstallSnapshot reply, ignore", rf, args.LastIncludedIndex, rf.log.LastIncludedIndex)
			return
		}
		if rf.status != Leader {
			Debug(dInfo, "%s is not Leader anymore, ignore InstallSnapshot reply", rf)
			return
		}
		rf.progress.nextIndex[i] = args.LastIncludedIndex + 1
		rf.progress.matchIndex[i] = args.LastIncludedIndex
	}
}

func (rf *Raft) apply() {
	for rf.killed() == false {
		rf.applyCond.L.Lock()
		for rf.commitIndex <= rf.lastApplied && len(rf.image) == 0 {
			rf.applyCond.Wait()
			if rf.killed() {
				rf.applyCond.L.Unlock()
				return
			}
		}

		tmp := make([]ApplyMsg, 0)
		if len(rf.image) != 0 {
			msg := ApplyMsg{
				CommandValid:  false,
				SnapshotValid: true,
				Snapshot:      rf.image,
				SnapshotTerm:  rf.log.LastIncludedTerm,
				SnapshotIndex: rf.log.LastIncludedIndex + 1,
			}
			tmp = append(tmp, msg)
			rf.image = nil
		}
		Debug(dInfo, "%s lastApplied is %d, commitIndex is %d, snapshot: (%d: %d)", rf, rf.lastApplied, rf.commitIndex, rf.log.LastIncludedIndex, rf.log.LastIncludedTerm)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			entry := rf.log.get(i)
			msg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index + 1,
			}
			tmp = append(tmp, msg)
			rf.lastApplied = i
		}
		rf.applyCond.L.Unlock()

		for i := range tmp {
			applyMsg := tmp[i]
			rf.applyCh <- applyMsg
			if applyMsg.CommandValid {
				Debug(dCommit, "%s applied %d index log", rf, applyMsg.CommandIndex-1)
			} else {
				Debug(dSnap, "%s install snapshot, lastIncludedIndex is %d, lastIncludedTerm is %d", rf, applyMsg.SnapshotIndex, applyMsg.SnapshotTerm)
			}
		}
	}
}

func (rf *Raft) becomeCandidate() {
	rf.status = Candidate
	rf.vote = rf.me
	rf.knock = time.Now()
	rf.term++

	rf.progress = &progress{
		poll:       make([]bool, len(rf.peers)),
		nextIndex:  make([]int, len(rf.peers)),
		matchIndex: make([]int, len(rf.peers)),
	}
	rf.progress.poll[rf.me] = true
	next := rf.log.nextIndex()
	for i := range rf.progress.nextIndex {
		rf.progress.nextIndex[i] = next
	}
	for i := range rf.progress.matchIndex {
		rf.progress.matchIndex[i] = rf.log.LastIncludedIndex
	}
}

func (rf *Raft) becomeFollower() {
	rf.status = Follower
	rf.progress = nil
	rf.immedCh = nil
}

func (rf *Raft) becomeLeader() {
	rf.status = Leader
	rf.immedCh = make(chan struct{}, 1)
	rf.knock = time.Now()
	rf.isLeaderCond.Signal()
}

func (rf *Raft) String() string {
	return fmt.Sprintf("S%d %s T%d", rf.me, rf.status, rf.term)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.vote = None
	rf.log.LastIncludedIndex = None
	rf.log.LastIncludedTerm = None
	rf.commitIndex = None
	rf.lastApplied = None
	rf.status = Follower
	rf.knock = time.Now()
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.isLeaderCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.restoreSnapshot(persister.ReadSnapshot())
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartBeatIfIsLeader()
	go rf.apply()

	return rf
}

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
