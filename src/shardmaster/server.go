package shardmaster


import (
	"../kvraft"
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"fmt"
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Debug = 0

	JOIN  = 4
	LEAVE = 5
	MOVE  = 6
	QUERY = 7
)

func DPrintf(format string, a ...interface{}) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type ShardMaster struct {
	mu      sync.Mutex
	cond    *sync.Cond
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	maxRaftState int

	// Your data here.
	nextSeq   map[int64]int
	snapDoing bool

	lastAppliedIndex  int
	lastAppliedTerm   int
	lastIncludedIndex int
	sleepCnt          *kvraft.SleepCounter

	configs []Config // indexed by config num
}


type Op struct {
	// Your data here.
	OpType  int
	Seq     int
	CliID   int64
	GIDs    []int
	Servers map[int][]string // new GID -> servers mappings
	GID     int
	Num     int // desired config number
	Shard   int
}

type Snapshot struct {
	configs          []Config
	NextSeq          map[int64]int
	LastIncludedIdx  int
	LastIncludedTerm int
}

// Invoke with kv.mu holding
func (sm *ShardMaster) DoSnapshot() {
	sm.snapDoing = true
	for !sm.sleepCnt.CheckSleep(sm.lastAppliedIndex) {
		// Should wait until handler routines all wake up!
		sm.cond.Wait()
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(sm.configs)
	e.Encode(sm.nextSeq)
	sm.lastIncludedIndex = sm.lastAppliedIndex
	e.Encode(sm.lastIncludedIndex)
	e.Encode(sm.lastAppliedTerm)
	snapRaw := w.Bytes()
	sm.rf.DoSnapshot(sm.lastIncludedIndex, sm.maxRaftState, snapRaw)
	sm.snapDoing = false
}

// RaftReqAndApply send a log appending request and wait for this log
// to be applied or fail (by witness the actual log at idx
func (sm *ShardMaster) RaftReqAndApply(op *Op, cb func()) Err{
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	idx, initTerm, isLeader := sm.rf.Start(*op)
	if !isLeader {
		DPrintf("[SM] %v is not leader\n", sm.me)
		return ErrWrongLeader
	}

	DPrintf("[SM] Submit cmd %+v to Raft core, leader=%v\n", op, sm.me)
	sm.sleepCnt.Add(idx)
	defer func() {
		sm.sleepCnt.Sub(idx)
		sm.cond.Broadcast()
	}()

	for !sm.killed() {
		curTerm, isLeader := sm.rf.GetState()
		if !isLeader || curTerm != initTerm {
			return ErrWrongLeader
		}

		fetchedLog := sm.rf.FetchLogContent(idx)
		if sm.lastAppliedIndex >= idx && fetchedLog != nil {
			if realLog, ok := fetchedLog.(Op); ok {
				if realLog.CliID == op.CliID &&
					realLog.Seq == op.Seq {
					if cb != nil {
						cb()
					}
					DPrintf("[SM] Leader %v process cmd %+v ok, will resp to client\n", sm.me, op)
					return OK
				}
				DPrintf("[SM] Server %v send ErrWrongLeader to server %v\n", sm.me, op.CliID)
				return ErrWrongLeader
			}

			fmt.Println("Error ret type of FetchLogContent")
			return ErrType
		}

		sm.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
		sm.mu.Lock()
	}

	return ErrUnknown
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := &Op{
		OpType:  JOIN,
		Seq:     args.Seq,
		CliID:   args.CliID,
		Servers: args.Servers,
	}

	reply.Err = sm.RaftReqAndApply(op, nil)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := &Op{
		OpType:  LEAVE,
		Seq:     args.Seq,
		CliID:   args.CliID,
		GIDs:    args.GIDs,
	}

	reply.Err = sm.RaftReqAndApply(op, nil)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := &Op{
		OpType: MOVE,
		Seq:    args.Seq,
		CliID:  args.CliID,
		Shard:  args.Shard,
		GID:    args.GID,
	}

	reply.Err = sm.RaftReqAndApply(op, nil)
}

func copyConfig(src *Config, dest *Config) {
	dest.Num = src.Num
	dest.Shards = src.Shards

	dest.Groups = make(map[int][]string)
	for k, v := range src.Groups {
		dest.Groups[k] = make([]string, len(v))
		copy(dest.Groups[k], v)
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := &Op{
		OpType: QUERY,
		Seq:    args.Seq,
		CliID:  args.CliID,
		Num:    args.Num,
	}

	reply.Err = sm.RaftReqAndApply(op, func() {
		if op.Num == -1 || op.Num > len(sm.configs) - 1 {
			copyConfig(&sm.configs[len(sm.configs)-1], &reply.Config)
		} else {
			copyConfig(&sm.configs[op.Num], &reply.Config)
		}
	})
	DPrintf("[SM] Leader %v Query resp=%+v\n", sm.me, reply)
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

func (sm *ShardMaster) killed() bool {
	z := atomic.LoadInt32(&sm.dead)
	return z == 1
}

func (sm *ShardMaster) wrappedRebalance(oldShards [NShards]int, oldGrpMap,
	newGrpMap map[int][]string) [NShards]int {
	var oldGrp, newGrp []int
	for k, _ := range oldGrpMap {
		oldGrp = append(oldGrp, k)
	}
	for k, _ := range newGrpMap {
		newGrp = append(newGrp, k)
	}
	DPrintf("[SM] Server %v starts rebalance: oldShards=%+v, oldGrps=%+v, newGrps=%+v\n", sm.me, oldShards, oldGrp, newGrp)
	newShards, _ := rebalance(oldShards, oldGrp, newGrp)
	DPrintf("[SM] Server %v finishes rebalance: newShards=%+v\n", sm.me, newShards)
	return newShards
}

func (sm *ShardMaster) genConfigJoin(servers map[int][]string) {
	oldConfig := &sm.configs[len(sm.configs)-1]
	newConfig := Config{
		Num: len(sm.configs),
		Shards: oldConfig.Shards,
		Groups: make(map[int][]string),
	}

	for k, v := range servers {
		newConfig.Groups[k] = make([]string, len(v))
		copy(newConfig.Groups[k], v)
	}

	for k, v := range oldConfig.Groups {
		newConfig.Groups[k] = make([]string, len(v))
		copy(newConfig.Groups[k], v)
	}

	newConfig.Shards = sm.wrappedRebalance(oldConfig.Shards,
		oldConfig.Groups, newConfig.Groups)

	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) genConfigLeave(GIDs []int) {
	newConfig := Config{}
	oldConfig := &sm.configs[len(sm.configs)-1]
	copyConfig(&sm.configs[len(sm.configs)-1], &newConfig)
	newConfig.Num = len(sm.configs)

	// Deep copy of shards map
	for _, gid := range GIDs {
		delete(newConfig.Groups, gid)
	}

	newConfig.Shards = sm.wrappedRebalance(oldConfig.Shards,
		oldConfig.Groups, newConfig.Groups)

	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) genConfigMove(shardID int, GID int) {
	newConfig := Config{}
	copyConfig(&sm.configs[len(sm.configs)-1], &newConfig)
	newConfig.Num = len(sm.configs)
	newConfig.Shards[shardID] = GID

	sm.configs = append(sm.configs, newConfig)
}


func (sm *ShardMaster) ApplyChListener() {
	for !sm.killed() {
		newMsg := <-sm.applyCh
		sm.mu.Lock()

		if applyMsgOp, ok := newMsg.Command.(Op); ok {
			// A normal log entry
			if applyMsgOp.Seq < sm.nextSeq[applyMsgOp.CliID] {
				// Dup detection: just ignore dup cmd in rf.log
			} else {
				sm.nextSeq[applyMsgOp.CliID] = applyMsgOp.Seq + 1
				switch applyMsgOp.OpType {
				case JOIN:
					sm.genConfigJoin(applyMsgOp.Servers)
				case LEAVE:
					sm.genConfigLeave(applyMsgOp.GIDs)
				case MOVE:
					sm.genConfigMove(applyMsgOp.Shard, applyMsgOp.GID)
				case QUERY: // Do nothing
				}
			}
			DPrintf("[SM] Server %v applied log %v: %v\n", sm.me, newMsg.CommandIndex, newMsg.Command)
			sm.lastAppliedIndex = newMsg.CommandIndex
			sm.lastAppliedTerm = newMsg.CommandTerm
			sm.cond.Broadcast()
		} else if snapBytes, ok := newMsg.Command.([]byte); ok {
			// Snapshot cmd
			var snapMsg Snapshot
			r := bytes.NewBuffer(snapBytes)
			d := labgob.NewDecoder(r)

			if d.Decode(&snapMsg.configs) != nil {
				panic("Error: Decode snapshot configs error")
			}
			if d.Decode(&snapMsg.NextSeq) != nil {
				panic("Decode seqMap error")
			}
			if d.Decode(&snapMsg.LastIncludedIdx) != nil {
				panic("Decode lastIncludedIdx error")
			}

			sm.configs = snapMsg.configs
			sm.nextSeq = snapMsg.NextSeq
			sm.lastAppliedIndex = snapMsg.LastIncludedIdx
			sm.lastAppliedTerm = snapMsg.LastIncludedTerm
			sm.cond.Broadcast()
			DPrintf("Server %v applied snapshot, lastIncIdx=%v\n",
				sm.me, snapMsg.LastIncludedIdx)
		} else {
			DPrintf("Error: illegal msg type(%v)\n", newMsg.Command)
			panic(fmt.Sprintf("Error: illegal msg type(%v)\n", newMsg.Command))
		}

		// Optionally do snapshot
		if sm.maxRaftState != -1 && sm.rf.RaftStateSize() >= sm.maxRaftState && !sm.snapDoing {
			DPrintf("[SM] Server %v snapshot with %v logs and size %v\n",
				sm.me, sm.lastAppliedIndex, sm.rf.RaftStateSize())
			sm.DoSnapshot()
		}
		sm.mu.Unlock()
	}
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := &ShardMaster{
		mu:           sync.Mutex{},
		me:           me,
		applyCh:      make(chan raft.ApplyMsg),
		maxRaftState: math.MaxInt32,
		nextSeq:      make(map[int64]int),
		sleepCnt: &kvraft.SleepCounter{
			SleepN: make(map[int]bool),
		},
		configs: make([]Config, 1),
	}

	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.cond = sync.NewCond(&sm.mu)

	var configs []Config
	var seqMap map[int64]int
	var lastIncludedIdx, lastIncludedTerm int

	dataBytes := sm.rf.ReadSnapshot()
	if dataBytes != nil && len(dataBytes) >= 1 {
		r := bytes.NewBuffer(dataBytes)
		d := labgob.NewDecoder(r)

		if d.Decode(&configs) != nil {
			panic("Decode snapshot data error")
		}
		if d.Decode(&seqMap) != nil {
			panic("Decode seqMap error")
		}
		if d.Decode(&lastIncludedIdx) != nil {
			panic("Decode lastIncludedIdx error")
		}
		if d.Decode(&lastIncludedTerm) != nil {
			panic("Decode lastIncludedTerm error")
		}
		sm.rf.SetSnapshotParam(lastIncludedIdx, lastIncludedTerm)
	}

	// Deep copy
	for _, conf := range configs {
		var newConf Config
		copyConfig(&conf, &newConf)
		sm.configs = append(sm.configs, newConf)
	}
	for k, v := range seqMap {
		sm.nextSeq[k] = v
	}
	DPrintf("[SM] Server %v started\n", sm.me)
	go sm.ApplyChListener()

	return sm
}
