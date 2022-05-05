package shardkv


// import "../shardmaster"
import (
	"../kvraft"
	"../labrpc"
	"../shardmaster"
	"bytes"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)
import "../raft"
import "sync"
import "../labgob"

const (
	Debug = 0

	GET    = 0
	PUT    = 1
	APPEND = 2
	CONFIG = 3
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
	OpType   int
	Key      string
	Value    string
	SnapData []byte
	CliID    int64
	Seq      int
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

type ShardKV struct {
	mu           sync.Mutex
	cond         *sync.Cond
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	dead         int32

	// Your definitions here.
	data      map[string]string
	nextSeq   map[int64]int
	snapDoing bool

	lastAppliedIndex  int
	lastAppliedTerm   int
	lastIncludedIndex int
	sleepCnt          *kvraft.SleepCounter
	mck               *shardmaster.Clerk
	myShards          map[int]bool
}

// Invoke with kv.mu holding
func (kv *ShardKV) DoSnapshot() {
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

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		OpType: GET,
		Key:    args.Key,
		CliID:  args.CliID,
		Seq:    args.Seq,
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	idx, initTerm, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("[KV] %v is not leader\n", kv.me)
		return
	}

	// Decline other shards
	keyShard := key2shard(args.Key)
	if _, ok := kv.myShards[keyShard]; !ok {
		reply.Err = ErrWrongGroup
		DPrintf("[KV] %v-%v cannot serve shard %v, args=%v\n",
			kv.gid, kv.me, keyShard, args)
		return
	}
	DPrintf("[KV] Get cmd %+v ok, leader=%v\n",
		args, kv.me)

	// Keep watching until this log has been applied in kv.rf.logs
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
					reply.Value = kv.data[args.Key]
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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
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

	// Decline other shards
	keyShard := key2shard(args.Key)
	if _, ok := kv.myShards[keyShard]; !ok {
		reply.Err = ErrWrongGroup
		DPrintf("[KV] %v-%v cannot serve shard %v, args=%v\n",
			kv.gid, kv.me, keyShard, args)
		return
	}
	DPrintf("[KV] PA cmd %+v ok, leader=%v\n",
		args, kv.me)

	kv.sleepCnt.Add(idx)
	defer func() {
		kv.sleepCnt.Sub(idx)
		kv.cond.Broadcast()
	}()

	// Keep watching until this log is appied in kv.rf.logs
	for !kv.killed() {
		curTerm, isLeader := kv.rf.GetState()
		if !isLeader || curTerm != initTerm {
			reply.Err = ErrWrongLeader
			return
		}

		fetchedLog := kv.rf.FetchLogContent(idx)
		if kv.lastAppliedIndex >= idx && fetchedLog != nil {
			if realLog, ok := fetchedLog.(Op); ok {
				if realLog.CliID == op.CliID && realLog.Seq == op.Seq { // Success
					reply.Err = OK
				} else { // Fail
					reply.Err = ErrWrongLeader
				}
			} else {
				panic(fmt.Sprintf("Error type %v(%T) of FetchLogContent",
					fetchedLog, fetchedLog))
			}
			break
		}

		kv.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
		kv.mu.Lock()
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) ApplyChListener() {
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
			DPrintf("[KV] Server %v-%v applied log %v: %+v\n", kv.gid, kv.me,
				newMsg.CommandIndex, newMsg.Command)
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
			DPrintf("Server %v-%v applied snapshot, lastIncIdx=%v\n",
				kv.gid, kv.me, snapMsg.LastIncludedIdx)
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

func (kv *ShardKV) PollConfig() {
	for {
		config := kv.mck.Query(-1)
		var addShards, rmShards []int
		kv.mu.Lock()
		for shard, gid := range config.Shards {
			if gid == kv.gid {
				if _, ok := kv.myShards[shard]; !ok {
					// Shard assigned to my group
					addShards = append(addShards, shard)
					kv.myShards[shard] = true
				}
			} else {
				if _, ok := kv.myShards[shard]; ok {
					// Shard taken away from my group
					rmShards = append(rmShards, shard)
					delete(kv.myShards, shard)
				}
			}
		}

		if len(rmShards) > 0 || len(addShards) > 0 {
			DPrintf("[KV] Server %v-%v query config %+v, newShards=%+v, rmShards=%+v\n",
				kv.gid, kv.me, config, addShards, rmShards)
		}

		time.Sleep(80 * time.Millisecond)
		kv.mu.Unlock()
	}
}

//
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
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := &ShardKV{
		mu:           sync.Mutex{},
		me:           me,
		applyCh:      make(chan raft.ApplyMsg),
		make_end:     make_end,
		gid:          gid,
		masters:      masters,
		maxraftstate: maxraftstate,
		data:         make(map[string]string),
		nextSeq:      make(map[int64]int),
		sleepCnt: &kvraft.SleepCounter{
			SleepN: make(map[int]bool),
		},
		mck:      shardmaster.MakeClerk(masters),
		myShards: make(map[int]bool),
	}

	// Your initialization code here.
	kv.cond = sync.NewCond(&kv.mu)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)

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

	DPrintf("Server %v-%v started with maxraftstate=%v\n", kv.gid, kv.me, maxraftstate)
	go kv.ApplyChListener()
	go kv.PollConfig()

	return kv
}
