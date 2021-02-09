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
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex // Lock to protect shared access to this peer's state
	cond      *sync.Cond
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
}

func (rf *Raft) PrintLogs() string {
	logStr := "<len=" + fmt.Sprintf("%v", len(rf.log))
	// for i := 0; i < len(rf.log); i++ {
	// 	logStr = fmt.Sprintf("%v, %v(%v)", logStr, rf.log[i].Content, rf.log[i].Term)
	// }
	logStr += ">"
	return logStr
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
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	var currentTermD int
	var votedForD int
	var logD []*LogEntry
	if d.Decode(&currentTermD) != nil {
		fmt.Println("Decode currentTerm error")
	} else {
		rf.currentTerm = currentTermD
	}

	if d.Decode(&votedForD) != nil {
		fmt.Println("Decode votedFor error")
	} else {
		rf.votedFor = votedForD
	}

	if d.Decode(&logD) != nil {
		fmt.Println("Decode log error")
	} else {
		rf.log = logD
	}
	DPrintf("[PER] %v(%v) recovers with log %v", rf.me, rf.currentTerm, rf.PrintLogs())
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
		DPrintf("[ELECTION] Ins %v(%v) recv reqVote from %v(%v), not grant 1\n", rf.me,
			rf.currentTerm, args.CandidateId, args.Term)
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
		DPrintf("[ELECTION] Ins %v(%v) recv reqVote from %v(%v), not grant 2\n", rf.me,
			rf.currentTerm, args.CandidateId, args.Term)
		return
	}

	// Up-to-date or not?
	if len(rf.log) > 0 {
		lastLog := rf.log[len(rf.log)-1]
		if args.LastLogTerm < lastLog.Term ||
			(args.LastLogTerm == lastLog.Term && args.LastLogIndex < len(rf.log)) {
			// Not up-to-date
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			DPrintf("[ELECTION] Ins %v(%v) recv reqVote from %v(%v), not grant 3, recv <t,id>=<%v,%v>, my log: %v\n",
				rf.me, rf.currentTerm, args.CandidateId, args.Term,
				args.LastLogTerm, args.LastLogIndex, rf.PrintLogs())
			return
		}
	}

	rf.votedFor = args.CandidateId
	rf.electionTimer.Reset(RandGenerator(LOWER_BOUND, UPPER_BOUND))
	reply.VoteGranted = true
	reply.Term = rf.currentTerm
	DPrintf("[ELECTION] Ins %v(%v) recv reqVote from %v, granted. My log: %v\n",
		rf.me, rf.currentTerm, args.CandidateId, rf.PrintLogs())
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
	DPrintf("[ELECTION] Ins %v send reqVote to %v\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		// Do nothing
		return ok
	}

	if reply.VoteGranted {
		rf.forNum++
	}
	rf.totNum++
	rf.cond.Broadcast()
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
	return fmt.Sprintf("term=%v, leaderId=%v, prevLogIdx=%v, prevLogTerm=%v, logs=(%v, %v], leaderCommit=%v",
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

func (rf *Raft) HeartbeatShooter() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.FollowerShooter(i)
	}
}

// Only launched in leader routine
func (rf *Raft) FollowerShooter(id int) {
	rf.mu.Lock()
	for !rf.killed() && rf.state == LEADER {
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
			PrevLogIndex: rf.nextIndex[id] - 1,
		}
		if args.PrevLogIndex > len(rf.log) {
			DPrintf("[ERROR] prevLogIndex(%v) out of log range(%v) to follower %v from leader %v(%v)",
				args.PrevLogIndex, len(rf.log), id, rf.me, rf.currentTerm)
		}
		if args.PrevLogIndex >= 1 {
			args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
		}

		lastLogIdx := len(rf.log)
		if lastLogIdx >= rf.nextIndex[id] {
			for i := rf.nextIndex[id]; i <= lastLogIdx; i++ {
				args.Entries = append(args.Entries, rf.log[i-1])
			}
		}
		reply := &AppendEntriesReply{}
		go rf.sendAppendEntries(id, args, reply)

		rf.mu.Unlock()
		time.Sleep(HEARTBEAT_INTERVAL * time.Millisecond)
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
			LastLogIndex: len(rf.log),
		}
		if len(rf.log) > 0 {
			args.LastLogTerm = rf.log[len(rf.log)-1].Term
		}
		reply := &RequestVoteReply{}
		go rf.sendRequestVote(i, args, reply)
	}

	// Wait for votes
	// RPC should return anyway(success or fail),
	//  otherwise it will block here indefinitely.
	// May concurrently receive heartbeat or reqVote which causes reduction.
	// So we check state for each loop.
	for rf.state == CANDIDATE && rf.forNum <= len(rf.peers)/2 && rf.totNum < len(rf.peers) {
		// To avoid permanent sleep, ensure holding the lock before lauching
		//  broadcast routine(to prevent the latter from broadcast before wait)
		rf.cond.Wait()
	}

	if rf.forNum > (len(rf.peers))/2 {
		// Win the election
		DPrintf("[ELECTION] Ins %v won election(%v) with %v grants and logs %v\n",
			rf.me, rf.currentTerm, rf.forNum, rf.PrintLogs())
		rf.state = LEADER
		rf.electionTimer.Stop()
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = len(rf.log) + 1
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
		DPrintf("[ELECTION] Ins %v timeout\n", rf.me)
		go rf.ElectionRunner()
	}
}

// Heartbeat handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer func() {
		rf.persist()
		rf.mu.Unlock()
	}()

	//DPrintf("Ins %v recv heartbeat from %v\n", rf.me, args.LeaderId)
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.Reason = OUTDATED_TERM
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
	if args.PrevLogIndex > 0 {
		if len(rf.log) < args.PrevLogIndex || rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			reply.Success = false
			reply.Term = rf.currentTerm

			if args.PrevLogIndex <= len(rf.log) {
				reply.ConflictIdx = args.PrevLogIndex
				reply.ConflictTerm = rf.log[args.PrevLogIndex-1].Term
				for k := reply.ConflictIdx - 1; k >= 0; k-- {
					if rf.log[k].Term == reply.ConflictTerm {
						reply.ConflictIdx = k + 1
					} else {
						break
					}
				}
			} else { // Some logs are missing
				reply.ConflictIdx = len(rf.log) + 1
			}
			reply.Reason = LOG_INCONSISTENCY
			return
		}
	}

	i := 0
	myPtr := args.PrevLogIndex
	for myPtr < len(rf.log) && i < len(args.Entries) && args.Entries[i].Term == rf.log[myPtr].Term {
		i++
		myPtr++
	}
	// Three cases:
	if myPtr >= len(rf.log) {
		// 1. Normal - just append new entries
		rf.log = append(rf.log, args.Entries[i:]...)
		// 2. Recv entries are outdated
	} else if i >= len(args.Entries) {
		// Ignore them
	} else if args.Entries[i].Term != rf.log[myPtr].Term {
		// 3. confliction
		DPrintf("[APP] %v(%v) conflict: id,t=<%v, %v> covered by <%v, %v> from %v(%v)\n",
			rf.me, rf.currentTerm, myPtr+1, rf.log[myPtr].Term,
			myPtr+1, args.Entries[i].Term, args.LeaderId, args.Term)
		rf.log = rf.log[:myPtr]
		rf.log = append(rf.log, args.Entries[i:]...)
	} else {
		fmt.Println("Error1023")
	}

	DPrintf("[APP] %v(%v) recv logs from %v(%v), (%v, %v] = %v",
		rf.me, rf.currentTerm, args.LeaderId, args.Term,
		len(rf.log)-len(args.Entries), len(rf.log), rf.PrintLogs())

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < len(rf.log) {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.log)
		}
	}

	reply.Success = true
	reply.Term = rf.currentTerm
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
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("[APP] %v(%v) -> %v with args %v; nextIdx=%v\n",
		rf.me, rf.currentTerm, server, args, rf.nextIndex)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		// Do nothing
		return ok
	}

	if reply.Success {
		lastIdx := len(args.Entries) + args.PrevLogIndex
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		// Check for update matchIndex
		for i := rf.commitIndex + 1; i <= lastIdx; i++ {
			if rf.log[i-1].Term != rf.currentTerm {
				continue
			}
			if ok, num := rf.replicatedByMajority(i); ok {
				rf.commitIndex = i
				rf.persist()
				DPrintf("[APP] %v(%v) commits log %v with %v replicas\n", rf.me,
					rf.currentTerm, i, num)
			}

		}
	} else if reply.Reason == LOG_INCONSISTENCY {
		DPrintf("[APP] %v(%v) is inconsistent with %v in prev log %v\n", server, rf.currentTerm, rf.me, args.PrevLogIndex)
		rf.nextIndex[server] = reply.ConflictIdx
		if rf.nextIndex[server] <= 0 {
			fmt.Println("Error: nextIndex is zero")
		}
		// TODO: retry immediately
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
		return -1, rf.currentTerm, false
	}

	newLogEntryPtr := &LogEntry{
		Content: command,
		Term:    rf.currentTerm,
		Index:   len(rf.log) + 1,
	}

	rf.log = append(rf.log, newLogEntryPtr)
	logIndex := len(rf.log)
	rf.matchIndex[rf.me] = len(rf.log)

	DPrintf("[SYS] New cmd %v recv at idx=%v(%v), val=%v, %v\n", rf.me,
		logIndex, rf.currentTerm, newLogEntryPtr.Content, rf.PrintLogs())

	return logIndex, rf.currentTerm, true
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
			msg := ApplyMsg{
				Command:      rf.log[newIdx-1].Content,
				CommandValid: true,
				CommandIndex: newIdx,
			}
			applyCh <- msg
			DPrintf("[SYS] %v(%v) applies log %v, val=%v\n", rf.me, rf.currentTerm,
				newIdx, msg.Command)
			rf.lastApplied++
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
		peers:         peers,
		persister:     persister,
		me:            me,
		state:         FOLLOWER,
		votedFor:      -1,
		commitIndex:   0,
		lastApplied:   0,
		currentTerm:   0,
		nextIndex:     make([]int, len(peers)),
		matchIndex:    make([]int, len(peers)),
		electionTimer: time.NewTimer(RandGenerator(LOWER_BOUND, UPPER_BOUND)),
	}
	rf.cond = sync.NewCond(&rf.mu)
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = len(rf.log) + 1
		rf.matchIndex[i] = 0
	}

	// Your initialization code here (2A, 2B, 2C).
	go rf.TimeoutWatcher()
	go rf.Applier(applyCh)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
