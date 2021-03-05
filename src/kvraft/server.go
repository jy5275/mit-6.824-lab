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

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType int
	Key    string
	Value  string
	CliID  int64
	Seq    int
}

func (op *Op) IsEqual(op2 *Op) bool {
	return op.CliID == op2.CliID && op.Seq == op2.Seq
}

type Snapshot struct {
	Data             map[string]string
	NextSeq          map[int64]int
	LastIncludedIdx  int
	LastIncludedTerm int
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
	data    map[string]string
	nextSeq map[int64]int

	lastAppliedIndex int
	lastIncludedTerm int
}

// Invoke with kv.mu holding
func (kv *KVServer) DoSnapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.nextSeq)
	e.Encode(kv.lastAppliedIndex)
	e.Encode(kv.lastIncludedTerm)
	snapRaw := w.Bytes()
	kv.rf.DoSnapshot(kv.lastAppliedIndex, snapRaw)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		OpType: GET,
		Key:    args.Key,
		Value:  "",
		CliID:  args.CliID,
		Seq:    args.Seq,
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	idx, initTerm, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("%v is not leader\n", kv.me)
		return
	}
	DPrintf("Get cmd{log=%v, <%v, %v>, seq=%v} ok, leader=%v, from cli %v\n",
		idx, op.Key, op.Value, args.Seq, kv.me, args.CliID)

	// Keep watching until this log is appied
	for !kv.killed() {
		curTerm, isLeader := kv.rf.GetState()
		if !isLeader || curTerm != initTerm {
			reply.Err = ErrWrongLeader
			return
		}

		log := kv.rf.FetchLogContent(idx)
		if kv.lastAppliedIndex >= idx && log != nil {
			if realLog, ok := log.(Op); ok {
				// Log at idx has been applied
				// Client op must has succeed or failed.
				if realLog.CliID == op.CliID && realLog.Seq == op.Seq {
					// Success
					reply.Err = OK
					reply.Value = kv.data[args.Key]
				} else {
					// Fail
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
		// kv.cond.Wait()
	}
	if kv.maxraftstate != -1 && kv.rf.RaftStateSize() >= kv.maxraftstate {
		// TODO: Do snapshot
		kv.DoSnapshot()
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
		DPrintf("%v is not leader\n", kv.me)
		return
	}
	DPrintf("PA cmd{log=%v, type=%v, <%v, %v>, seq=%v} ok, leader=%v, from cli %v\n",
		idx, op.OpType, op.Key, op.Value, args.Seq, kv.me, args.CliID)

	// Keep watching until this log is appied in kv.rf.logs
	for !kv.killed() {
		curTerm, isLeader := kv.rf.GetState()
		if !isLeader || curTerm != initTerm {
			reply.Err = ErrWrongLeader
			return
		}

		log := kv.rf.FetchLogContent(idx)
		if kv.lastAppliedIndex >= idx && log != nil {
			if realLog, ok := log.(Op); ok {
				if realLog.CliID == op.CliID && realLog.Seq == op.Seq {
					// Success
					reply.Err = OK
				} else {
					// Fail
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
		// kv.cond.Wait()
	}
	if kv.maxraftstate != -1 && kv.rf.RaftStateSize() >= kv.maxraftstate {
		// TODO: Do snapshot
		kv.DoSnapshot()
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
			// Dup detection: just ignore dup cmd in rf.log
			if applyMsgOp.Seq < kv.nextSeq[applyMsgOp.CliID] {
				DPrintf("Dup cmd(type=%v) from cli %v, recv seq=%v, nextSeq[%v]=%v\n",
					applyMsgOp.OpType, applyMsgOp.CliID, applyMsgOp.Seq,
					applyMsgOp.CliID, kv.nextSeq[applyMsgOp.CliID])
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
			DPrintf("Server %v applied log %v: %v\n", kv.me, newMsg.CommandIndex, newMsg.Command)
			kv.lastAppliedIndex = newMsg.CommandIndex
			kv.lastIncludedTerm = newMsg.CommandTerm
			kv.cond.Broadcast()
		} else {
			fmt.Println("Error6824")
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

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.cond = sync.NewCond(&kv.mu)
	kv.data = make(map[string]string)
	kv.nextSeq = make(map[int64]int)

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	dataMap, seqMap, _, _ := kv.rf.ReadSnapshot()
	// Deep copy
	for k, v := range dataMap {
		kv.data[k] = v
	}
	for k, v := range seqMap {
		kv.nextSeq[k] = v
	}

	DPrintf("Server %v started...\n", kv.me)
	go kv.ApplyChListener()

	return kv
}
