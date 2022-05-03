package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

const (
	GET    = 0
	PUT    = 1
	APPEND = 2
	SNAP   = 3
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type SeqArray struct {
	mu       sync.Mutex
	Data     []int
	StartIdx int
}

func (sa *SeqArray) Witness(v int) {
	sa.mu.Lock()
	defer sa.mu.Unlock()
	nextIdx := sa.StartIdx + len(sa.Data)
	if v > nextIdx {
		for i := nextIdx; i < v; i++ {
			sa.Data = append(sa.Data, -i)
		}
		sa.Data = append(sa.Data, v)
	} else if v < nextIdx {
		localIdx := v - sa.StartIdx
		if localIdx < 0 {
			fmt.Printf("localIdx < 0, v=%v, startIdx=%v, arr=%v\n", v, sa.StartIdx, sa.Data)
			panic("localIdx < 0")
		}
		sa.Data[localIdx] = -sa.Data[localIdx]
	} else {
		sa.Data = append(sa.Data, v)
	}
}

func (sa *SeqArray) Pop() int {
	sa.mu.Lock()
	defer sa.mu.Unlock()
	if len(sa.Data) == 0 {
		return sa.StartIdx - 1
	}
	for i := 0; i < len(sa.Data); i++ {
		if sa.Data[i] < 0 {
			if i == 0 { // Unwitnessed logs in the front
				return sa.StartIdx - 1
			} else { // Unwitnessed logs
				maxWit := sa.Data[i-1]
				sa.StartIdx = -sa.Data[i]
				sa.Data = sa.Data[i:]
				return maxWit
			}
		}
	}
	sa.StartIdx = sa.Data[len(sa.Data)-1] + 1
	sa.Data = []int{}
	return sa.StartIdx - 1
}

type SleepCounter struct {
	mu     sync.Mutex
	SleepN map[int]bool
}

func (sc *SleepCounter) CheckSleep(v int) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	for k, _ := range sc.SleepN {
		if k <= v {
			return false
		}
	}
	return true
}

func (sc *SleepCounter) Add(v int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.SleepN[v] = true
}

func (sc *SleepCounter) Sub(v int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	delete(sc.SleepN, v)
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType   int
	Key      string
	Value    string
	SnapData []byte
	CliID    int64
	Seq      int
}

type Snapshot struct {
	Data             map[string]string
	NextSeq          map[int64]int
	LastIncludedIdx  int
	LastIncludedTerm int
}

func (op *Op) IsEqual(op2 *Op) bool {
	return op.CliID == op2.CliID && op.Seq == op2.Seq
}

type KVServer struct {
	mu      sync.Mutex // Cannot wait kv.mu while holding rf.mu!!!
	cond    *sync.Cond
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data      map[string]string
	nextSeq   map[int64]int
	snapDoing bool

	lastAppliedIndex  int
	lastAppliedTerm   int
	lastIncludedIndex int
	sleepCnt          *SleepCounter
}

// Invoke with kv.mu holding
func (kv *KVServer) DoSnapshot() {
	kv.snapDoing = true
	for !kv.sleepCnt.CheckSleep(kv.lastAppliedIndex) {
		// Should wait until handler routines all wake up!
		kv.cond.Wait()
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.nextSeq)
	kv.lastIncludedIndex = kv.lastAppliedIndex
	e.Encode(kv.lastIncludedIndex)
	e.Encode(kv.lastAppliedTerm)
	snapRaw := w.Bytes()
	kv.rf.DoSnapshot(kv.lastIncludedIndex, kv.maxraftstate, snapRaw)
	kv.snapDoing = false
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shouldCmtNops := kv.rf.ShouldCommitNops()

	if shouldCmtNops {
		op := Op{
			OpType: GET,
			Key:    args.Key,
			Value:  "",
			CliID:  args.CliID,
			Seq:    args.Seq,
		}

		idx, initTerm, isLeader := kv.rf.Start(op)
		if !isLeader {
			reply.Err = ErrWrongLeader
			DPrintf("[KV] %v is not leader\n", kv.me)
			return
		}
		DPrintf("[KV] Get cmd{log=%v, <%v, %v>, seq=%v} ok, leader=%v, from cli %v\n",
			idx, op.Key, op.Value, args.Seq, kv.me, args.CliID)

		// Keep watching until this log is appied
		kv.sleepCnt.Add(idx)

		for !kv.killed() {
			curTerm, isLeader := kv.rf.GetState()
			if !isLeader || curTerm != initTerm {
				reply.Err = ErrWrongLeader
				kv.sleepCnt.Sub(idx)
				kv.cond.Broadcast()
				return
			}

			log := kv.rf.FetchLogContent(idx)
			if kv.lastAppliedIndex >= idx && log != nil {
				if realLog, ok := log.(Op); ok {
					// Log at idx has been applied
					// Nops must has succeed or failed.
					kv.sleepCnt.Sub(idx)
					kv.cond.Broadcast()
					if realLog.CliID != op.CliID || realLog.Seq != op.Seq { // Fail
						reply.Err = ErrWrongLeader
						DPrintf("[KV] %v is not leader\n", kv.me)
						return
					}
				}
				break
			}

			kv.mu.Unlock()
			time.Sleep(50 * time.Millisecond)
			kv.mu.Lock()
		}
	}

	res, cmtIdx := kv.rf.LeaderRead()
	if !res {
		reply.Err = ErrWrongLeader
		DPrintf("[KV] %v is not leader\n", kv.me)
		return
	}

	DPrintf("[KV-DEBUG] Get cmd, key=%v, leader=%v, from cli %v, cmtIdx=%v\n", args.Key, kv.me, args.CliID, cmtIdx)

	// Keep watching until cmtIdx has been applied
	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			reply.Err = ErrWrongLeader
			DPrintf("[KV-DEBUG] Failed to get due to wrong leader, cmtIdx=%v, <k=%v>, leader=%v, from cli %v\n", cmtIdx, args.Key, kv.me, args.CliID)
			break
		}

		if kv.lastAppliedIndex >= cmtIdx {
			// Success
			reply.Err = OK
			reply.Value = kv.data[args.Key]
			DPrintf("[KV-DEBUG] Has seen the result of cmtIdx=%v, <k=%v, v=%v>, leader=%v, from cli %v\n", cmtIdx, args.Key, reply.Value, kv.me, args.CliID)
			break
		}
		kv.cond.Wait()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Key:   args.Key,
		Value: args.Value,
		CliID: args.CliID,
		Seq:   args.Seq,
	}
	switch args.Op {
	case "Put":
		op.OpType = PUT
	case "Append":
		op.OpType = APPEND
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	idx, initTerm, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("[KV] %v is not leader\n", kv.me)
		return
	}
	DPrintf("[KV] PA cmd{log=%v, type=%v, <%v, %v>, seq=%v} ok, leader=%v, from cli %v\n",
		idx, op.OpType, op.Key, op.Value, args.Seq, kv.me, args.CliID)

	// Keep watching until this log is appied in kv.rf.logs
	kv.sleepCnt.Add(idx)
	defer func() {
		kv.sleepCnt.Sub(idx)
		kv.cond.Broadcast()
	}()

	for !kv.killed() {
		curTerm, isLeader := kv.rf.GetState()
		if !isLeader || curTerm != initTerm {
			reply.Err = ErrWrongLeader
			return
		}

		log := kv.rf.FetchLogContent(idx)
		if kv.lastAppliedIndex >= idx && log != nil {
			if realLog, ok := log.(Op); ok {
				if realLog.CliID == op.CliID && realLog.Seq == op.Seq { // Success
					reply.Err = OK
				} else { // Fail
					reply.Err = ErrWrongLeader
				}
			} else {
				fmt.Println("Error ret type of FetchLogContent")
			}
			break
		}

		kv.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
		kv.mu.Lock()
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) ApplyChListener() {
	for !kv.killed() {
		newMsg := <-kv.applyCh
		kv.mu.Lock()

		if applyMsgOp, ok := newMsg.Command.(Op); ok {
			// A normal log entry
			if applyMsgOp.Seq < kv.nextSeq[applyMsgOp.CliID] {
				// Dup detection: just ignore dup cmd in rf.log
			} else {
				kv.nextSeq[applyMsgOp.CliID] = applyMsgOp.Seq + 1
				key := applyMsgOp.Key
				val := applyMsgOp.Value
				if applyMsgOp.OpType == PUT {
					kv.data[key] = val
				} else if applyMsgOp.OpType == APPEND {
					kv.data[key] += val
				}
			}
			DPrintf("[KV] Server %v applied log %v: %v\n", kv.me, newMsg.CommandIndex, newMsg.Command)
			kv.lastAppliedIndex = newMsg.CommandIndex
			kv.lastAppliedTerm = newMsg.CommandTerm
			kv.cond.Broadcast()
		} else if snapBytes, ok := newMsg.Command.([]byte); ok {
			// Snapshot cmd
			var snapMsg Snapshot
			r := bytes.NewBuffer(snapBytes)
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

			kv.data = snapMsg.Data
			kv.nextSeq = snapMsg.NextSeq
			kv.lastAppliedIndex = snapMsg.LastIncludedIdx
			kv.lastAppliedTerm = snapMsg.LastIncludedTerm
			kv.cond.Broadcast()
			DPrintf("Server %v applied snapshot, lastIncIdx=%v\n",
				kv.me, snapMsg.LastIncludedIdx)
		} else {
			DPrintf("Error: illegal msg type(%v)\n", newMsg.Command)
			panic(fmt.Sprintf("Error: illegal msg type(%v)\n", newMsg.Command))
		}

		// Optionally do snapshot
		if kv.maxraftstate != -1 && kv.rf.RaftStateSize() >= kv.maxraftstate && !kv.snapDoing {
			DPrintf("[KV] Server %v snapshot with %v logs and size %v\n",
				kv.me, kv.lastAppliedIndex, kv.rf.RaftStateSize())
			kv.DoSnapshot()
		}
		kv.mu.Unlock()
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := &KVServer{
		me:           me,
		maxraftstate: maxraftstate,
		data:         make(map[string]string),
		nextSeq:      make(map[int64]int),
		applyCh:      make(chan raft.ApplyMsg),
		snapDoing:    false,
	}

	// You may need initialization code here.
	kv.cond = sync.NewCond(&kv.mu)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.sleepCnt = &SleepCounter{
		SleepN: make(map[int]bool),
	}

	// You may need initialization code here.
	//dataMap, seqMap, _, _ := kv.rf.ReadSnapshot()
	var dataMap map[string]string
	var seqMap map[int64]int
	var lastIncludedIdx, lastIncludedTerm int

	dataBytes := kv.rf.ReadSnapshot()
	if dataBytes != nil && len(dataBytes) >= 1 {
		r := bytes.NewBuffer(dataBytes)
		d := labgob.NewDecoder(r)

		if d.Decode(&dataMap) != nil {
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
		kv.rf.SetSnapshotParam(lastIncludedIdx, lastIncludedTerm)
	}

	// Deep copy
	for k, v := range dataMap {
		kv.data[k] = v
	}
	for k, v := range seqMap {
		kv.nextSeq[k] = v
	}

	DPrintf("Server %v started with maxraftstate=%v\n", kv.me, maxraftstate)
	go kv.ApplyChListener()

	return kv
}
