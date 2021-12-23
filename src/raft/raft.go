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
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

// import "bytes"
// import "../labgob"

const (
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2
)

const (
	OUTDATED_TERM     = 1
	LOG_INCONSISTENCY = 2
)

const (
	LOWER_BOUND        = 200
	UPPER_BOUND        = 350
	HEARTBEAT_INTERVAL = 120
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
}

type Snapshot struct {
	Data             map[string]string
	NextSeq          map[int64]int
	LastIncludedIdx  int
	LastIncludedTerm int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex // Lock to protect shared access to this peer's state
	cond      *sync.Cond
	condApply *sync.Cond
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int // to whom I vote in this term
	log         []*LogEntry

	state       int
	forNum      int // supporting votes received in election
	totNum      int // total votes received in election
	commitIndex int
	lastApplied int

	nextIndex     []int
	matchIndex    []int
	electionTimer *time.Timer

	lastIncludedIdx  int
	lastIncludedTerm int
	applyCh          chan ApplyMsg
	Heartbeats       []chan chan bool
}

// Invoke with lock!
func (rf *Raft) GetLastLogIdx() int {
	if len(rf.log) > 0 {
		return rf.log[len(rf.log)-1].Index
	}

	// No log entries
	if rf.lastIncludedIdx > -1 {
		return rf.lastIncludedIdx
	}

	// Initial state
	return 0
}

// Invoke with lock!
func (rf *Raft) GetLastLogTerm() int {
	if len(rf.log) > 0 {
		return rf.log[len(rf.log)-1].Term
	}
	return rf.lastIncludedTerm
}

// Invoke with lock!
func (rf *Raft) PrintLogs() string {
	logStr := "<len=" + fmt.Sprintf("%v", len(rf.log))
	for i := 0; i < len(rf.log); i++ {
		logStr = fmt.Sprintf("%v, %v(Idx=%v, Term=%v)", logStr,
			rf.log[i].Content, rf.log[i].Index, rf.log[i].Term)
	}
	logStr += ">"
	return logStr
}

// Cvt raft log index to local array index
// If the log entry at index has been snapshotted and isn't lastIncludedIndex,
//  	an num < -1 will be returned.
// Invoke with lock!
func (rf *Raft) GetLocalIdx(index int) int {
	if len(rf.log) == 0 {
		if rf.lastIncludedIdx != index {
			// Error: cannot find localIdx for this index
			return -99
		}
		return -1 // For lastIdx in snapshot
	}
	localIdx := index - rf.log[0].Index
	return localIdx
}

// Invoke with lock!
func (rf *Raft) GetLogTerm(index int) int {
	if index == rf.lastIncludedIdx {
		return rf.lastIncludedTerm
	}
	localIdx := rf.GetLocalIdx(index)
	if localIdx < 0 || localIdx >= len(rf.log) {
		// Covered by snapshot
		return -99
	}
	return rf.log[localIdx].Term
}

// Invoke with lock!
func (rf *Raft) FetchLogByIdx(index int) *LogEntry {
	if len(rf.log) == 0 {
		return nil
	}
	localIdx := rf.GetLocalIdx(index)
	if localIdx >= len(rf.log) || localIdx < 0 {
		return nil
	}
	return rf.log[localIdx]
}

// @Const
func (rf *Raft) FetchLogContent(index int) interface{} {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log := rf.FetchLogByIdx(index)
	if log == nil {
		return nil
	}
	return log.Content
}

func (rf *Raft) RaftStateSize() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

func RandGenerator(s, e int) time.Duration {
	r := e - s
	randNum := rand.Intn(r) + s
	return time.Duration(randNum) * time.Millisecond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// Trim log at index and all logs before,
//  supersed them by snapshot in raw
func (rf *Raft) DoSnapshot(index, sizeLimit int, raw []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if sizeLimit != -1 && rf.persister.RaftStateSize() < sizeLimit {
		// fmt.Printf("sz=%v, limit=%v\n", rf.persister.RaftStateSize(), sizeLimit)
		return
	}

	localIdx := rf.GetLocalIdx(index)
	if localIdx < 0 {
		panic(fmt.Sprintf("[Snap] %v(%v) Raft.DoSnapshot(%v, %v, bytes), localIdx=%v", rf.me, rf.currentTerm, index, sizeLimit, localIdx))
	}
	rf.lastIncludedIdx = rf.log[localIdx].Index
	rf.lastIncludedTerm = rf.log[localIdx].Term
	rf.log = rf.log[localIdx+1:]

	w0 := new(bytes.Buffer)
	e0 := labgob.NewEncoder(w0)
	e0.Encode(rf.currentTerm)
	e0.Encode(rf.votedFor)
	e0.Encode(rf.log)
	state := w0.Bytes()
	DPrintf("[Snap] %v(%v) ready to trim, <lastIncIdx=%v, lastIncTerm=%v>, size:%v, log=%v", rf.me,
		rf.currentTerm, rf.lastIncludedIdx, rf.lastIncludedTerm, rf.persister.RaftStateSize(), rf.PrintLogs())

	rf.persister.SaveStateAndSnapshot(state, raw)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTermD int
	var votedForD int
	var logD []*LogEntry
	if d.Decode(&currentTermD) != nil {
		panic("Decode currentTerm error")
	} else {
		rf.currentTerm = currentTermD
	}

	if d.Decode(&votedForD) != nil {
		panic("Decode votedFor error")
	} else {
		rf.votedFor = votedForD
	}

	if d.Decode(&logD) != nil {
		panic("Decode log error")
	} else {
		rf.log = logD
	}
	DPrintf("[PER] %v(%v) recovers with log %v", rf.me, rf.currentTerm, rf.PrintLogs())
}

// Return value: data, nextSeq, lastIncludedIdx, lastIncludedTerm
func (rf *Raft) ReadSnapshot() (map[string]string, map[int64]int, int, int) {
	data := rf.persister.ReadSnapshot()
	if data == nil || len(data) < 1 {
		return nil, nil, -1, -1
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var snapMap map[string]string
	var seqMap map[int64]int

	if d.Decode(&snapMap) != nil {
		panic("Decode snapshot data error")
	}
	if d.Decode(&seqMap) != nil {
		panic("Decode seqMap error")
	}
	if d.Decode(&rf.lastIncludedIdx) != nil {
		panic("Decode lastIncludedIdx error")
	}
	if d.Decode(&rf.lastIncludedTerm) != nil {
		panic("Decode lastIncludedTerm error")
	}
	rf.lastApplied = rf.lastIncludedIdx
	DPrintf("[PER] %v(%v) read snapshot with data=%v, nextSeq=%v",
		rf.me, rf.currentTerm, snapMap, seqMap)
	return snapMap, seqMap, -1, -1
}

type LogEntry struct {
	Index   int
	Term    int
	Content interface{}
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

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer func() {
		rf.persist()
		rf.mu.Unlock()
	}()

	if args.Term < rf.currentTerm {
		// Outdated election, ignore it
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DPrintf("[ELECTION] Ins %v(%v) recv reqVote from %v(%v), NOT due to outdated term\n",
			rf.me, rf.currentTerm, args.CandidateId, args.Term)
		return
	}

	// Unconditional reduction
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
	}

	// Has already voted for another server in this term
	if rf.votedFor != -1 {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DPrintf("[ELECTION] Ins %v(%v) recv reqVote from %v(%v), NOT due to voted\n", rf.me,
			rf.currentTerm, args.CandidateId, args.Term)
		return
	}

	// Up-to-date or not?
	if len(rf.log) > 0 || rf.lastIncludedIdx > 0 {
		if args.LastLogTerm < rf.GetLastLogTerm() ||
			(args.LastLogTerm == rf.GetLastLogTerm() && args.LastLogIndex < rf.GetLastLogIdx()) {
			// Not up-to-date
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			DPrintf("[ELECTION] Ins %v(%v) recv reqVote from %v(%v), NOT, recv <t,id>=<%v,%v>, my log: %v\n",
				rf.me, rf.currentTerm, args.CandidateId, args.Term,
				args.LastLogTerm, args.LastLogIndex, rf.PrintLogs())
			return
		}
	}

	rf.votedFor = args.CandidateId
	rf.electionTimer.Reset(RandGenerator(LOWER_BOUND, UPPER_BOUND))
	reply.VoteGranted = true
	reply.Term = rf.currentTerm
	DPrintf("[ELECTION] Ins %v(%v) recv reqVote from %v, granted.\n",
		rf.me, rf.currentTerm, args.CandidateId)
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
	DPrintf("[RV-->] Ins %v(%v) send reqVote to %v\n", rf.me, args.Term, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.cond.Broadcast()
	if args.Term < rf.currentTerm {
		DPrintf("[RV===>]A delayed sendRV package from can %v(%v)\n",
			rf.me, args.Term)
		return ok
	}

	// Unconditional reduction
	if reply.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
		return ok
	}

	if reply.VoteGranted {
		rf.forNum++
	}
	rf.totNum++
	// rf.cond.Broadcast()
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}

func (a *AppendEntriesArgs) String() string {
	return fmt.Sprintf("<Term=%v, LeaderId=%v, PrevLogIdx=%v, PrevLogTerm=%v, Logs=(%v, %v], LeaderCommit=%v>",
		a.Term, a.LeaderId, a.PrevLogIndex, a.PrevLogTerm, a.PrevLogIndex,
		a.PrevLogIndex+len(a.Entries), a.LeaderCommit)
}

type AppendEntriesReply struct {
	Term         int
	Success      bool
	Reason       int
	ConflictTerm int
	ConflictIdx  int
}

func (a *AppendEntriesReply) String() string {
	return fmt.Sprintf("<Term=%v, Success=%v, Reason=%v, ConflictTerm=%v, ConflictIdx=%v>",
		a.Term, a.Success, a.Reason, a.ConflictTerm, a.ConflictIdx)
}

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludedIdx  int
	LastIncludedTerm int
	Data             []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) HeartbeatShooter() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.FollowerShooter(i)
	}
}

func (rf *Raft) LeaderRead() (bool, int) {
	// OK without mutex locked
	_, isLeader := rf.GetState()
	if !isLeader {
		return false, -1
	}
	rf.mu.Lock()
	leaderCmtIdx := rf.commitIndex
	rf.mu.Unlock()

	resultCh := make(chan bool, len(rf.peers))
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.Heartbeats[i] <- resultCh
	}

	recvCnt := 1
	followCnt := 1
	for recvCnt < len(rf.peers) {
		select {
		case result := <-resultCh:
			recvCnt++
			if result == true {
				followCnt++
			}
			DPrintf("[DEBUG] Get result: %v, cur recv=%v, follow=%v\n", result, recvCnt, followCnt)
		default:
			rf.mu.Lock()
			if rf.state != LEADER { // TODO: state use atomic var
				rf.mu.Unlock()
				return false, -1
			}
			rf.mu.Unlock()
			time.Sleep(20 * time.Millisecond)
		}
	}

	return followCnt > len(rf.peers)/2, leaderCmtIdx
}

// Only launched in leader routine
func (rf *Raft) FollowerShooter(id int) {
	rf.mu.Lock()
	var leaderCollect chan bool
	for !rf.killed() && rf.state == LEADER {
		prevLogIdx := rf.nextIndex[id] - 1
		if rf.GetLastLogIdx() > 0 && rf.GetLogTerm(prevLogIdx) < 0 && rf.lastIncludedIdx > 0 {
			// Send installSnapshot RPC
			args := &InstallSnapshotArgs{
				Term:             rf.currentTerm,
				LeaderId:         rf.me,
				LastIncludedIdx:  rf.lastIncludedIdx,
				LastIncludedTerm: rf.lastIncludedTerm,
				Data:             rf.persister.ReadSnapshot(),
			}
			reply := &InstallSnapshotReply{}
			go rf.sendInstallSnapshot(id, args, reply, leaderCollect)
		} else {
			// Send AppendEntries
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.commitIndex,
				PrevLogIndex: prevLogIdx,
			}
			reply := &AppendEntriesReply{}
			if args.PrevLogIndex > 0 {
				// OK, this isn't the first log entry...
				args.PrevLogTerm = rf.GetLogTerm(args.PrevLogIndex)
			}

			// Need to carry some logs in this RPC
			if rf.GetLastLogIdx() >= rf.nextIndex[id] {
				for i := rf.nextIndex[id]; i <= rf.GetLastLogIdx(); i++ {
					logAtI := rf.FetchLogByIdx(i)
					args.Entries = append(args.Entries, logAtI)
				}
			}
			go rf.sendAppendEntries(id, args, reply, leaderCollect)
		}
		rf.mu.Unlock()
		select {
		case leaderCollect = <-rf.Heartbeats[id]:
			break
		case <-time.After(HEARTBEAT_INTERVAL * time.Millisecond):
			leaderCollect = nil
			break
		}
		rf.mu.Lock()
	}
	rf.mu.Unlock()
}

func (rf *Raft) ElectionRunner() {
	rf.mu.Lock()
	defer func() {
		rf.persist()
		rf.mu.Unlock()
	}()

	// Start an election
	rf.currentTerm++
	thisTerm := rf.currentTerm
	rf.state = CANDIDATE
	rf.votedFor = rf.me
	rf.forNum = 1
	rf.totNum = 1
	rf.electionTimer.Reset(RandGenerator(LOWER_BOUND, UPPER_BOUND)) // Prepare for re-election
	rf.persist()

	// Send requestVote to all servers (except myself)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: rf.GetLastLogIdx(),
		}
		if args.LastLogIndex > 0 {
			args.LastLogTerm = rf.GetLastLogTerm()
		}
		reply := &RequestVoteReply{}
		go rf.sendRequestVote(i, args, reply)
	}

	// Wait for votes
	// RPC should return anyway(success or fail),
	//  otherwise it will block here indefinitely.
	// May concurrently receive heartbeat or reqVote which causes reduction.
	// So we check state for each loop.
	for rf.state == CANDIDATE && rf.forNum <= len(rf.peers)/2 && rf.totNum < len(rf.peers) && rf.currentTerm == thisTerm {
		// To avoid permanent sleep, ensure holding the lock before lauching
		//  broadcast routine(to prevent the latter from broadcast before wait)
		rf.cond.Wait()
		DPrintf("[RV] Can %v(%v) has collected %v grants\n", rf.me, thisTerm, rf.forNum)
	}
	if rf.state != CANDIDATE || rf.currentTerm != thisTerm {
		// Reduce to follower or another election has been launched!
		return
	}

	if rf.forNum > (len(rf.peers))/2 {
		// Win the election
		DPrintf("[ELECTION] Ins %v won election(%v) with %v grants and logs %v\n",
			rf.me, thisTerm, rf.forNum, rf.PrintLogs())
		rf.state = LEADER
		// rf.electionTimer.Stop()
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = rf.GetLastLogIdx() + 1
			rf.matchIndex[i] = 0
		}
		go rf.HeartbeatShooter()
	} else {
		// No enough supportting votes. Possible causes:
		// 1. Split vote, just wait another ElectionRunner routine to launch.
		// 2. Fail the election. Someone else preceds.
		// On either cases, just end this routine naively.
	}
}

// Listen on timeout event
func (rf *Raft) TimeoutWatcher() {
	for !rf.killed() {
		<-rf.electionTimer.C
		rf.mu.Lock()
		if rf.state != LEADER {
			DPrintf("[ELECTION] Ins %v timeout\n", rf.me)
			go rf.ElectionRunner()
		}
		rf.electionTimer.Reset(RandGenerator(LOWER_BOUND, UPPER_BOUND))
		rf.mu.Unlock()
	}
}

// Heartbeat handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer func() {
		rf.persist()
		rf.mu.Unlock()
	}()

	DPrintf("[->AppEnt] %v(%v) recv AppEnt: %v, curLogLen=%v, processing...\n",
		rf.me, rf.currentTerm, args, len(rf.log))
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.Reason = OUTDATED_TERM
		DPrintf("[->AppEnt] %v(%v) decline AppEnt from %v(%v) due to outdated term\n",
			rf.me, rf.currentTerm, args.LeaderId, args.Term)
		return
	}

	// Unconditional reduction
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
	}

	// Reset timer
	rf.electionTimer.Reset(RandGenerator(LOWER_BOUND, UPPER_BOUND))

	// Check prev log match
	if args.PrevLogIndex > 0 && args.PrevLogIndex > rf.lastIncludedIdx {
		if rf.GetLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
			reply.Success = false
			reply.Term = rf.currentTerm

			if args.PrevLogIndex <= rf.GetLastLogIdx() {
				// Conflict prevLogTerm! Find the FIRST log entry with this wrong term
				reply.ConflictIdx = args.PrevLogIndex
				reply.ConflictTerm = rf.GetLogTerm(args.PrevLogIndex)
				// Backtrack all preceding log entries (until step into snapshot)
				for k := reply.ConflictIdx; k >= 0 && rf.GetLocalIdx(k) >= 0; k-- {
					if rf.GetLogTerm(k) == reply.ConflictTerm {
						reply.ConflictIdx = k
					} else {
						break
					}
				}
			} else {
				// Some logs are missing
				reply.ConflictIdx = rf.GetLastLogIdx() + 1
			}
			reply.Reason = LOG_INCONSISTENCY
			DPrintf("[->AppEnt] %v(%v) decline AppEnt from %v(%v) due to incons, conflictIdx=%v, cmtIdx=%v, my log=%v\n",
				rf.me, rf.currentTerm, args.LeaderId, args.Term, reply.ConflictIdx, rf.commitIndex, rf.PrintLogs())
			return
		}
	}

	lenCoveredBySnap := rf.lastIncludedIdx - args.PrevLogIndex
	if lenCoveredBySnap < 0 {
		lenCoveredBySnap = 0
	}

	i := lenCoveredBySnap                             // idx of recved log array
	myPtr := args.PrevLogIndex + 1 + lenCoveredBySnap // idx of the log array on me
	for myPtr <= rf.GetLastLogIdx() && i < len(args.Entries) &&
		args.Entries[i].Term == rf.GetLogTerm(myPtr) {
		i++
		myPtr++
	}
	// Three cases:
	if myPtr > rf.GetLastLogIdx() {
		// 1. Normal - just append new entries
		rf.log = append(rf.log, args.Entries[i:]...)
		// 2. Recv entries are outdated
	} else if i >= len(args.Entries) {
		// Ignore them
	} else if args.Entries[i].Term != rf.GetLogTerm(myPtr) {
		// 3. confliction
		DPrintf("[AppEnt] %v(%v) conflict: id,t=<%v,%v> covered by <%v,%v> from %v(%v)\n",
			rf.me, rf.currentTerm, myPtr, rf.GetLogTerm(myPtr),
			myPtr, args.Entries[i].Term, args.LeaderId, args.Term)
		localIdx := rf.GetLocalIdx(myPtr)
		rf.log = rf.log[:localIdx]
		rf.log = append(rf.log, args.Entries[i:]...)
	} else {
		panic("Error1023")
	}

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < rf.GetLastLogIdx() {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.GetLastLogIdx()
		}
	}

	reply.Success = true
	reply.Term = rf.currentTerm

	DPrintf("[->AppEnt] %v(%v) reply ok to AppEnt recv from %v(%v): %v with cmtIdx=%v and log=%v\n",
		rf.me, rf.currentTerm, args.LeaderId, args.Term, reply, rf.commitIndex, rf.PrintLogs())
}

func (rf *Raft) replicatedByMajority(idx int) (bool, int) {
	cnt := 0
	for f := 0; f < len(rf.peers); f++ {
		if rf.matchIndex[f] >= idx {
			cnt++
		}
	}
	if cnt > len(rf.peers)/2 {
		return true, cnt
	}
	return false, cnt
}

// Leader only
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, c chan bool) bool {
	DPrintf("[AppEnt-->] %v(%v)->%v with args %v; nextIdx=%v\n",
		rf.me, rf.currentTerm, server, args, rf.nextIndex)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		// Do nothing
		DPrintf("[AppEnt===>]A delayed sendAppendEntries package from leader %v(%v)\n",
			rf.me, args.Term)
		if c != nil {
			c <- false
		}
		return ok
	}

	// Unconditional reduction
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
	}

	if reply.Success {
		lastIdx := len(args.Entries) + args.PrevLogIndex
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		// Check for update matchIndex
		for i := rf.commitIndex + 1; i <= lastIdx; i++ {
			if rf.GetLogTerm(i) != rf.currentTerm {
				continue
			}
			if ok, num := rf.replicatedByMajority(i); ok {
				rf.commitIndex = i
				rf.persist()
				DPrintf("[AppEnt===>] %v(%v) commits log %v with %v replicas\n", rf.me,
					args.Term, i, num)
			}
		}
		if c != nil {
			c <- true
		}
	} else if reply.Reason == LOG_INCONSISTENCY {
		DPrintf("[AppEnt===>] %v(%v) is inconsistent with %v in prev log %v\n", server, rf.currentTerm, rf.me, reply.ConflictIdx)
		rf.nextIndex[server] = reply.ConflictIdx
		if rf.nextIndex[server] <= 0 {
			panic("Error: nextIndex is zero")
		}
		if c != nil {
			c <- true
		}
		rf.Heartbeats[server] <- nil
	} else if reply.Reason == OUTDATED_TERM {
		DPrintf("[AppEnt===>] %v(%v)'s was declined by %v(%v) due to outdated term\n",
			rf.me, args.Term, server, reply.Term)
		if c != nil {
			c <- false
		}
	} else {
		DPrintf("[ERR] Unknown decline reason of sendAppEnt %v(%v)->%v(%v): %v\n",
			rf.me, args.Term, server, reply.Term, reply.Reason)
		if c != nil {
			c <- false
		}
	}
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	DPrintf("[->Snap] %v(%v) recv InSnap from %v(%v), my log=%v\n",
		rf.me, rf.currentTerm, args.LeaderId, args.Term, rf.PrintLogs())
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
	}
	rf.electionTimer.Reset(RandGenerator(LOWER_BOUND, UPPER_BOUND))

	// Ignore any outdated snapshot
	if args.LastIncludedIdx <= rf.lastIncludedIdx {
		rf.mu.Unlock()
		return
	}

	// 5. Save snapshot file, discard any existing or partial snapshot
	//    with a smaller index
	// 6. If existing log entry has same index and term as snapshot’s
	//    last included entry, retain log entries FOLLOWING it and reply
	if rf.GetLogTerm(args.LastIncludedIdx) == args.LastIncludedTerm {
		if rf.commitIndex < args.LastIncludedIdx {
			rf.commitIndex = args.LastIncludedIdx
		}
		cutIdx := args.LastIncludedIdx
		if rf.lastApplied < args.LastIncludedIdx {
			cutIdx = rf.lastApplied
		}
		localIdx := rf.GetLocalIdx(cutIdx)
		rf.log = rf.log[localIdx+1:]

		w0 := new(bytes.Buffer) // Raft state encoder
		e0 := labgob.NewEncoder(w0)
		e0.Encode(rf.currentTerm)
		e0.Encode(rf.votedFor)
		e0.Encode(rf.log)
		state := w0.Bytes()
		rf.lastIncludedIdx = args.LastIncludedIdx
		rf.lastIncludedTerm = args.LastIncludedTerm
		rf.persister.SaveStateAndSnapshot(state, args.Data)
		rf.mu.Unlock()
		return
	}

	// Log missing or conflict at LastIncIdx
	DPrintf("[->Snap] %v(%v) is incons with snap lastIncIdx=%v, lastIncT=%v, discard all logs\n",
		rf.me, rf.currentTerm, args.LastIncludedIdx, args.LastIncludedTerm)

	// 7. Discard the entire log
	rf.log = []*LogEntry{}
	w0 := new(bytes.Buffer) // Raft state encoder
	e0 := labgob.NewEncoder(w0)
	e0.Encode(rf.currentTerm)
	e0.Encode(rf.votedFor)
	e0.Encode(rf.log)
	state := w0.Bytes()
	rf.lastIncludedIdx = args.LastIncludedIdx
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.persister.SaveStateAndSnapshot(state, args.Data)

	// 8. Reset state machine using snapshot contents (and load
	//    snapshot’s cluster configuration)
	var snapMsg Snapshot
	r := bytes.NewBuffer(args.Data)
	d := labgob.NewDecoder(r)
	if d.Decode(&snapMsg.Data) != nil {
		panic("Error: Decode snapshot data error")
	}
	if d.Decode(&snapMsg.NextSeq) != nil {
		panic("Decode seqMap error")
	}
	if d.Decode(&snapMsg.LastIncludedIdx) != nil {
		panic("Decode lastIncludedIdx error")
	}

	msg := ApplyMsg{
		CommandValid: false,
		Command:      snapMsg,
	}
	rf.commitIndex = args.LastIncludedIdx
	rf.lastApplied = args.LastIncludedIdx
	rf.mu.Unlock()

	// It's ok to override all log msgs before current commitIndex
	rf.applyCh <- msg
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs,
	reply *InstallSnapshotReply, c chan bool) bool {
	DPrintf("[InSnap-->] %v(%v)->%v with args %v\n", rf.me,
		rf.currentTerm, server, args)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		if c != nil {
			c <- false
		}
		return ok
	}
	if args.Term < rf.currentTerm {
		// Delayed network. Do nothing
		DPrintf("[InSnap===>] A delayed sendInstallSnapshot package from leader %v(%v)\n",
			rf.me, args.Term)
		if c != nil {
			c <- false
		}
		return ok
	}

	if reply.Term > rf.currentTerm {
		// Outdated
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
		if c != nil {
			c <- false
		}
		return ok
	}

	rf.matchIndex[server] = args.LastIncludedIdx
	rf.nextIndex[server] = rf.matchIndex[server] + 1
	DPrintf("[InSnap===>] %v(%v) send InSnap to %v(%v) ok, matchIdx=%v, nextIdx=%v\n",
		rf.me, rf.currentTerm, server, reply.Term, rf.matchIndex, rf.nextIndex)
	if c != nil {
		c <- true
	}
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

	// Your code here (2B).
	rf.mu.Lock()
	defer func() {
		rf.persist()
		rf.mu.Unlock()
	}()

	if rf.state != LEADER {
		//DPrintf("[SYS] %v(%v) recv a cmd, but rejected due to not leader\n", rf.me, rf.currentTerm)
		return -1, rf.currentTerm, false
	}

	lastIndex := 0
	if len(rf.log) > 0 || rf.lastIncludedIdx != -1 {
		lastIndex = rf.GetLastLogIdx()
	}

	newLogEntryPtr := &LogEntry{
		Content: command,
		Term:    rf.currentTerm,
		Index:   lastIndex + 1,
	}

	rf.log = append(rf.log, newLogEntryPtr)
	newLogIndex := newLogEntryPtr.Index
	rf.matchIndex[rf.me] = newLogEntryPtr.Index

	DPrintf("[SYS] New cmd %v(%v) recv at idx=%v, val=%v, %v\n", rf.me,
		rf.currentTerm, newLogIndex, newLogEntryPtr.Content, rf.PrintLogs())

	return newLogIndex, rf.currentTerm, true
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) Applier(applyCh chan ApplyMsg) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		for rf.lastApplied < rf.commitIndex {
			newIdx := rf.lastApplied + 1
			if newIdx == 0 {
				// Addition log
				rf.lastApplied++
				rf.condApply.Broadcast()
				continue
			}
			if rf.FetchLogByIdx(newIdx) == nil {
				panic(fmt.Sprintf("%v(%v) cannot found log at %v, my curLog=%v, cmtIdx=%v\n",
					rf.me, rf.currentTerm, newIdx, rf.PrintLogs(), rf.commitIndex))
			}
			msg := ApplyMsg{
				Command:      rf.FetchLogByIdx(newIdx).Content,
				CommandValid: true,
				CommandIndex: newIdx,
				CommandTerm:  rf.GetLogTerm(newIdx),
			}
			DPrintf("[SYS] %v(%v) applies log %v ok, val=%v, lastApplied=%v, cmtIdx=%v\n",
				rf.me, rf.currentTerm, newIdx, msg.Command, rf.lastApplied, rf.commitIndex)
			rf.mu.Unlock()
			applyCh <- msg
			rf.mu.Lock()
			rf.lastApplied++
			rf.condApply.Broadcast()
		}
		rf.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
		rf.mu.Lock()
	}
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
	DPrintf("[SYS] Start ins %v\n", me)
	rf := &Raft{
		peers:            peers,
		persister:        persister,
		me:               me,
		state:            FOLLOWER,
		votedFor:         -1,
		commitIndex:      0,
		lastApplied:      -1,
		currentTerm:      0,
		nextIndex:        make([]int, len(peers)),
		matchIndex:       make([]int, len(peers)),
		electionTimer:    time.NewTimer(RandGenerator(LOWER_BOUND, UPPER_BOUND)),
		lastIncludedIdx:  -1,
		lastIncludedTerm: -1,
		applyCh:          applyCh,
		Heartbeats:       make([]chan chan bool, len(peers)),
	}
	for i := range rf.Heartbeats {
		rf.Heartbeats[i] = make(chan chan bool)
	}

	rf.cond = sync.NewCond(&rf.mu)
	rf.condApply = sync.NewCond(&rf.mu)

	rf.log = append(rf.log, &LogEntry{
		Index:   0,
		Term:    0,
		Content: nil,
	})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = rf.GetLastLogIdx() + 1
		rf.matchIndex[i] = 0
	}

	// Your initialization code here (2A, 2B, 2C).
	go rf.TimeoutWatcher()
	go rf.Applier(applyCh)

	return rf
}
