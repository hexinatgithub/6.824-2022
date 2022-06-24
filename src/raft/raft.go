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

	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
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
	Entries []Entry
}

func (l *Log) append(entry Entry) int {
	entry.Index = l.nextIndex()
	l.Entries = append(l.Entries, entry)
	return entry.Index
}

func (l *Log) replace(start int, entries ...Entry) {
	if len(entries) == 0 {
		return
	}

	last := l.last()
	i := 0
	for start <= last.Index {
		l.Entries[start] = entries[i]
		start++
		i++
	}
	for ; i < len(entries); i++ {
		l.Entries = append(l.Entries, entries[i])
	}
}

func (l *Log) nextIndex() int {
	return len(l.Entries)
}

func (l *Log) slice(start int) []Entry {
	next := l.nextIndex()
	if start > next {
		panic("slice")
	}
	src := l.Entries[start:]
	dst := make([]Entry, len(src))
	copy(dst, src)
	return dst
}

func (l *Log) firstIndexWithSameTerm(back int) int {
	i, j := back, back-1
	for i >= 0 {
		if l.get(i).Term != l.get(j).Term {
			return i
		}
		i--
		j--
	}
	return -1
}

func (l *Log) lastIndexWithSameTerm(start int) int {
	lastIndex := l.last().Index
	i, j := start, start+1
	for j <= lastIndex {
		if l.get(i).Term != l.get(j).Term {
			return i
		}
		i++
		j++
	}
	return lastIndex
}

var defaultEntry = &Entry{
	Index: -1,
	Term:  -1,
}
var empty *Entry = nil

func (l *Log) get(i int) *Entry {
	if i == -1 {
		return defaultEntry
	}

	next := l.nextIndex()
	if i >= next {
		return empty
	}
	result := l.Entries[i]
	return &result
}

func (l *Log) truncate(i int) {
	next := l.nextIndex()
	if i > next {
		panic("truncate")
	}
	l.Entries = l.Entries[:i]
}

func (l *Log) last() *Entry {
	return l.get(len(l.Entries) - 1)
}

func (l *Log) moreUpToDate(index, term int) bool {
	last := l.last()
	if last.Term != term {
		return last.Term > term
	}
	return last.Index > index
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

	commitIndex int
	lastApplied int
	status      status
	knock       time.Time
	progress    *progress
	applyCh     chan<- ApplyMsg
	applyCond   *sync.Cond
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

	if args.Term < rf.term {
		Debug(dDrop, "S%d %s RequestVote args's Term %d is old, current term is %d", rf.me, rf.status, args.Term, rf.term)
		reply.Term = rf.term
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.term {
		Debug(dTerm, "S%d %s RequestVote args's Term %d is new, current term is %d, change to %d", rf.me, rf.status, args.Term, rf.term, args.Term)
		rf.term = args.Term
		rf.vote = -1
		rf.becomeFollower()
	}

	// repeat request vote, grant
	if rf.vote == args.CandidateId {
		Debug(dVote, "S%d %s, already vote for %d, grant vote", rf.me, rf.status, args.CandidateId, rf.vote)
		reply.Term = rf.term
		reply.VoteGranted = true
		return
	}

	// vote for other peer, just deny
	if rf.vote != -1 {
		Debug(dVote, "S%d %s, deny vote for %d, already vote for %d", rf.me, rf.status, args.CandidateId, rf.vote)
		reply.Term = rf.term
		reply.VoteGranted = false
		return
	}

	if rf.log.moreUpToDate(args.LastLogIndex, args.LastLogTerm) {
		Debug(dVote, "S%d %s log is more update, deny vote for %d", rf.me, rf.status, args.CandidateId)
		reply.Term = rf.term
		reply.VoteGranted = false
		return
	}

	rf.vote = args.CandidateId
	rf.becomeFollower()
	reply.Term = rf.term
	reply.VoteGranted = true
	Debug(dVote, "S%d %s grant vote for %d, become Follower", rf.me, rf.status, args.CandidateId)
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.term {
		reply.Term = rf.term
		reply.Success = false
		Debug(dDrop, "S%d %s, AppendEntries args's Term %d is old, current term is %d",
			rf.me, rf.status, args.Term, rf.term)
		return
	}

	if args.Term > rf.term {
		rf.term = args.Term
		Debug(dTerm, "S%d %s, AppendEntries args's Term is new, current term is %d, change to %d, become Follower",
			rf.me, rf.status, rf.term, args.Term)
	}

	if rf.status != Follower {
		Debug(dInfo, "S%d %s transfer to %s", rf.me, rf.status, Follower)
		rf.becomeFollower()
	} else {
		rf.knock = time.Now()
		Debug(dInfo, "S%d %s receive AppendEntries", rf.me, rf.status)
	}

	entry := rf.log.get(args.PrevLogIndex)
	last := rf.log.last()

	// follower log is longer
	if entry == nil {
		reply.Term = rf.term
		reply.Success = false
		reply.XIndex = -1
		reply.XTerm = -1
		reply.XLen = last.Index + 1
		Debug(dLog, "S%d %s doesn't contain an entry at %d", rf.me, rf.status, args.PrevLogIndex)
		return
	}

	// conflict
	if entry.Term != args.PrevLogTerm {
		firstIndex := rf.log.firstIndexWithSameTerm(entry.Index)
		rf.log.truncate(args.PrevLogIndex)
		reply.Term = rf.term
		reply.Success = false
		reply.XIndex = firstIndex
		reply.XTerm = entry.Term
		reply.XLen = args.PrevLogIndex
		Debug(dLog, "S%d %s contain conflict entry at %d, truncate log", rf.me, rf.status, args.PrevLogIndex)
		return
	}

	rf.log.replace(args.PrevLogIndex+1, args.Entries...)
	last = rf.log.last()
	Debug(dLog, "S%d %s append new %d log entries", rf.me, rf.status, len(args.Entries))
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, last.Index)
		rf.applyCond.Signal()
		Debug(dCommit, "S%d %s update commitIndex to %d", rf.me, rf.status, rf.commitIndex)
	}

	reply.Term = rf.term
	reply.Success = true
	return
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
		Debug(dDrop, "S%d %s is not Leader, drop command", rf.me, rf.status)
		return index, term, false
	}

	index = rf.log.append(Entry{
		Term:    rf.term,
		Command: command,
	})
	term = rf.term
	Debug(dInfo, "S%d %s append log at index %d, current term is %d", rf.me, rf.status, index, term)

	rf.progress.nextIndex[rf.me] = index + 1
	rf.progress.matchIndex[rf.me] = index
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

const heartBeatInterval = 100 * time.Millisecond

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		randomize := time.Duration(2+rand.Intn(5)) * heartBeatInterval
		time.Sleep(randomize)

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.status == Leader {
				return
			}

			if time.Now().Sub(rf.knock) < randomize {
				return
			}

			Debug(dInfo, "S%d %s start election, become Candidate", rf.me, rf.status)
			rf.becomeCandidate()

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
			Debug(dDrop, "S%d %s is killed, drop RequestVote reply", rf.me, rf.status)
			return
		} else if reply.Term > rf.term {
			Debug(dTerm, "S%d %s, sendRequestVote reply contain a new Term %d, become Follower",
				rf.me, rf.status, reply.Term)
			rf.term = reply.Term
			rf.becomeFollower()
		} else if reply.Term != rf.term {
			Debug(dDrop, "S%d %s, sendRequestVote reply contain a old Term %d, drop message",
				rf.me, rf.status, reply.Term)
			return
		}

		if rf.status == Leader {
			Debug(dDrop, "S%d %s is already Leader, ignore S%d sendRequestVote reply", rf.me, rf.status, i)
			return
		}

		if reply.VoteGranted {
			rf.progress.poll[i] = true
			Debug(dVote, "S%d %s, S%d grant vote", rf.me, rf.status, i)
			if rf.progress.win() {
				Debug(dLeader, "S%d %s collect majority vote, become Leader", rf.me, rf.status)
				rf.becomeLeader()
			}
		}
	}
}

func (rf *Raft) heartBeatIfIsLeader() {
	for rf.killed() == false {
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.status == Leader {
				Debug(dLeader, "S%d %s begin send heartbeat to all peers", rf.me, rf.status)
				for i := range rf.peers {
					if i == rf.me {
						continue
					}

					next := rf.progress.nextIndex[i]
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
					Debug(dInfo, "S%d %s sendAppendEntries to S%d, nextIndex is %d, entries length is %d",
						rf.me, rf.status, i, next, len(entries))
					go rf.handleAppendEntries(i, &args, &reply)
				}
			}
		}()

		time.Sleep(heartBeatInterval)
	}
}

func (rf *Raft) handleAppendEntries(i int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.sendAppendEntries(i, args, reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.killed() {
			Debug(dDrop, "S%d %s is killed, drop sendAppendEntries reply", rf.me, rf.status)
			return
		}

		if reply.Term > rf.term {
			Debug(dTerm, "S%d %s sendAppendEntries reply contain new Term %d, become Follower", rf.me, rf.status, reply.Term)
			rf.term = reply.Term
			rf.becomeFollower()
		} else if reply.Term != rf.term {
			Debug(dDrop, "S%d %s sendAppendEntries reply contain old Term %d, current Term is %d, drop message", rf.me, rf.status, reply.Term, rf.term)
			reply.Term = rf.term
			reply.Success = false
			return
		}

		if rf.status != Leader {
			Debug(dLog2, "S%d %s is not Leader any more, ignore sendAppendEntries reply", rf.me, rf.status)
			return
		}

		if rf.progress.nextIndex[i] != args.PrevLogIndex+1 {
			Debug(dLog2, "S%d %s out of order sendAppendEntries reply, ignore")
			return
		}

		if !reply.Success {
			if reply.XLen < args.PrevLogIndex+1 {
				rf.progress.nextIndex[i] = reply.XLen
				Debug(dLog, "S%d %s follower S%d log is too short, quick roll back to %d", rf.me, rf.status, i, rf.progress.nextIndex[i])
				return
			}

			entry := rf.log.get(reply.XIndex)
			if entry.Term != reply.Term {
				rf.progress.nextIndex[i] = reply.XIndex
				Debug(dLog, "S%d %s doesn't have XTerm %d, quick roll back S%d nextIndex to %d", rf.me, rf.status, reply.Term, i, rf.progress.nextIndex[i])
				return
			}
			rf.progress.nextIndex[i] = rf.log.lastIndexWithSameTerm(reply.XIndex)
			Debug(dLog, "S%d %s contain XTerm %d, set S%d nextIndex to %d", rf.me, rf.status, entry.Term, i, rf.progress.nextIndex[i])
			return
		}

		if len(args.Entries) == 0 {
			Debug(dLog2, "S%d %s heartbeat reply, ignore", rf.me, rf.status)
			return
		}

		rf.progress.nextIndex[i] = args.PrevLogIndex + len(args.Entries) + 1
		rf.progress.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
		Debug(dLog, "S%d %s sendAppendEntries reply success, set S%d nextIndex to %d, matchIndex to %d", rf.me, rf.status, i, rf.progress.nextIndex[i], rf.progress.matchIndex[i])

		commitIndex := rf.progress.commitIndex()
		commitLog := rf.log.get(commitIndex)
		if commitLog.Term != rf.term {
			Debug(dCommit, "S%d %s %d commitIndex's Term is %d, current Term is %d, index will not commit", rf.me, rf.status, rf.commitIndex, commitLog.Term, rf.term)
			return
		}
		if commitIndex > rf.lastApplied {
			rf.commitIndex = commitIndex
			rf.applyCond.Signal()
			Debug(dCommit, "S%d %s update commitIndex to %d", rf.me, rf.status, rf.commitIndex)
		}
	}
}

func (rf *Raft) apply() {
	rf.applyCond.L.Lock()
	defer rf.applyCond.L.Unlock()
	for rf.killed() == false {
		if rf.commitIndex == rf.lastApplied {
			rf.applyCond.Wait()
		}

		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			entry := rf.log.get(i)
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index + 1,
			}
			rf.applyCh <- applyMsg
			rf.lastApplied = i
			Debug(dCommit, "S%d %s committed %d index log", rf.me, rf.status, i)
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
		rf.progress.matchIndex[i] = -1
	}
}

func (rf *Raft) becomeFollower() {
	rf.status = Follower
	rf.progress = nil
	rf.knock = time.Now()
}

func (rf *Raft) becomeLeader() {
	rf.status = Leader
	rf.knock = time.Now()
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
	rf.vote = -1
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.status = Follower
	rf.knock = time.Now()
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
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
