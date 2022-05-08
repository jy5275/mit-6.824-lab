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

	GET     = 0
	PUT     = 1
	APPEND  = 2
	RESHARD = 3
)

func DPrintf(format string, a ...interface{}) {
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

	// TODO: two logs for each re-sharding: StopOldShards --> ReShard
	// Send FetchShards RPC in phase2
	// Phase1:
	//  1. Start StopOldShards log, wait for apply
	//  2. In applier, remove old shards out of serving set
	// Phase2:
	//  3. FetchShards from other groups (say, G2)
	//     Retry on failure (possibly at this time G2 hasn't known the newest config from shardmaster)
	//  4. Start ReShard log, wait for apply
	//  5. In applier, add new shards into serving set and KVs
	//
	// FetchShards:
	//   1. If args.Num < localNum: discard
	//   2. If StopOldShards log has already been applied for Num: reply with data and return
	//   3. Reject and client retries
	MovedKVs  map[string]string // ReShard
	NewShards []int             // ReShard
	RmShards  []int             // StopOldShards
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
	myConfig          *shardmaster.Config
	cliID             int64
	seq               int
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
		DPrintf("[KV] %v-%v is not leader\n", kv.gid, kv.me)
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
	DPrintf("[KV] %v-%v receives GET cmd %+v, shard=%v\n", kv.gid, kv.me, args, keyShard)

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
			DPrintf("[KV] %v-%v is not leader\n", kv.gid, kv.me)
			return
		}

		fetchedLog := kv.rf.FetchLogContent(idx)
		if kv.lastAppliedIndex >= idx && fetchedLog != nil {
			if realLog, ok := fetchedLog.(Op); ok {
				if realLog.CliID == op.CliID && realLog.Seq == op.Seq { // Success
					reply.Err = OK
					reply.Value = kv.data[args.Key]
				} else { // Fail
					reply.Err = ErrWrongLeader
					DPrintf("[KV] %v-%v is not leader\n", kv.gid, kv.me)
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
		DPrintf("[KV] %v-%v is not leader\n", kv.gid, kv.me)
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
	DPrintf("[KV] %v-%v receives PA cmd %+v, shard=%v\n", kv.gid, kv.me, args, keyShard)

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
			DPrintf("[KV] %v-%v is not leader\n", kv.gid, kv.me)
			return
		}

		if _, ok := kv.myShards[keyShard]; !ok {
			reply.Err = ErrWrongGroup
			DPrintf("[KV] %v-%v cannot serve shard %v, args=%v\n",
				kv.gid, kv.me, keyShard, args)
			return
		}

		fetchedLog := kv.rf.FetchLogContent(idx)
		if kv.lastAppliedIndex >= idx && fetchedLog != nil {
			if realLog, ok := fetchedLog.(Op); ok {
				if realLog.CliID == op.CliID && realLog.Seq == op.Seq { // Success
					reply.Err = OK
				} else { // Fail
					reply.Err = ErrWrongLeader
					DPrintf("[KV] %v-%v is not leader\n", kv.gid, kv.me)
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

func (kv *ShardKV) FetchShards(args *FetchShardsArgs, reply *FetchShardsReply) {
	// Not necessary to wait for args.ConfNum to come
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("[KV] %v-%v receives FetchShard req %+v\n", kv.gid, kv.me, args)

	if args.ConfNum < kv.myConfig.Num {
		// Ignore outdated requests
		reply.Err = ErrOutdated
		DPrintf("[KV] %v-%v rejects FetchShard req %+v due to outdated num, my num is %v\n",
			kv.gid, kv.me, args, kv.myConfig.Num)
		return
	}

	for shard, _ := range args.NeedShards {
		delete(kv.myShards, shard)
	}

	reply.MovedKVs = make(map[string]string)
	for k, v := range kv.data {
		sh := key2shard(k)
		if _, ok := args.NeedShards[sh]; ok {
			reply.MovedKVs[k] = v
		}
	}
	reply.Err = OK
	DPrintf("[KV] %v-%v replies FetchShard RPC with %v\n",
		kv.gid, kv.me, reply)
}

// Should invoke without mutex lock!
func (kv *ShardKV) sendFetchShards(serverList []string, confNum int, needShards map[int]bool) (map[string]string, Err) {
	args := &FetchShardsArgs{
		NeedShards: needShards,
		ConfNum:    confNum,
	}
	reply := &FetchShardsReply{}

	for leaderID := 0; ; leaderID = (leaderID + 1) % len(serverList) {
		DPrintf("[KV] %v-%v ready to send FetchShard RPC to %v %+v\n",
			kv.gid, kv.me, serverList[leaderID], args)
		ok := kv.make_end(serverList[leaderID]).Call("ShardKV.FetchShards", args, reply)
		DPrintf("[KV] %v-%v receives FetchShard RPC reply: %+v\n",
			kv.gid, kv.me, reply)

		if ok && reply.Err == OK {
			return reply.MovedKVs, OK
		}

		if reply.Err == ErrWrongLeader {
			continue
		}

		if len(reply.Err) > 0 {
			return nil, reply.Err
		}

		time.Sleep(50 * time.Millisecond)
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
				switch applyMsgOp.OpType {
				case PUT:
					// TODO: check ErrWrongGroup
					kv.data[key] = val
				case APPEND:
					kv.data[key] += val
					DPrintf("[KV] %v-%v append result <%v, %v>\n", kv.gid, kv.me, key, kv.data[key])
				case RESHARD:
					for k, v := range applyMsgOp.MovedKVs {
						kv.data[k] = v
					}
					for _, shard := range applyMsgOp.NewShards {
						kv.myShards[shard] = true
					}
					for _, shard := range applyMsgOp.RmShards {
						delete(kv.myShards, shard)
					}
				}
			}
			DPrintf("[KV] %v-%v already applied log %v: %+v\n", kv.gid, kv.me,
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
			DPrintf("[KV] %v-%v snapshot with %v logs and size %v\n",
				kv.gid, kv.me, kv.lastAppliedIndex, kv.rf.RaftStateSize())
			kv.DoSnapshot()
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) getAllKVsFromOtherGrps(needShardsAllGrps map[int]map[int]bool, oldGrps map[int][]string,
	curNum int) map[string]string {
	newKVs := map[string]string{}
	newKVsCh := make(chan map[string]string, 10)

	for oldGID, shardSet := range needShardsAllGrps {
		serverList := oldGrps[oldGID]
		shardSetCache := shardSet
		go func() {
			fetchedKVs, errMsg := kv.sendFetchShards(serverList, curNum, shardSetCache)
			if len(errMsg) != 0 && errMsg != OK {
				DPrintf("[KV] fatal: failed to sendFetchShards, errMsg=%v\n", errMsg)
				panic("failed to sendFetchShards, errMsg=" + errMsg)
			}
			newKVsCh <- fetchedKVs
		}()
	}

	for i := 0; i < len(needShardsAllGrps); i++ {
		data := <-newKVsCh
		if data == nil {
			continue
		}
		for k, v := range data {
			newKVs[k] = v
		}
	}

	return newKVs
}

func (kv *ShardKV) AppendAReShardLog(op *Op) {
	idx, initTerm, isLeader := kv.rf.Start(*op)
	DPrintf("[KV] %v-%v tries to start a re-sharding log: %+v, idx=%v\n",
		kv.gid, kv.me, op, idx)

	if !isLeader {
		DPrintf("[KV] %v-%v is not leader, discard re-sharding log: %+v\n",
			kv.gid, kv.me, op)
		// Do nothing...
		return
	}

	kv.sleepCnt.Add(idx)
	defer func() {
		kv.sleepCnt.Sub(idx)
		kv.cond.Broadcast()
	}()

	kv.mu.Lock()
	defer kv.mu.Unlock()
	for !kv.killed() {
		curTerm, isLeader := kv.rf.GetState()
		if !isLeader || curTerm != initTerm {
			return
		}

		fetchedLog := kv.rf.FetchLogContent(idx)
		if kv.lastAppliedIndex >= idx && fetchedLog != nil {
			if realLog, ok := fetchedLog.(Op); ok {
				DPrintf("[KV] %v-%v has witnessed the log at idx=%v: %+v\n",
					kv.gid, kv.me, idx, realLog)
				if realLog.CliID == op.CliID && realLog.Seq == op.Seq { // Success, TODO
				} else { // Fail
				}
			} else {
				panic("Error ret type of FetchLogContent")
			}
			break
		}

		kv.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
		kv.mu.Lock()
	}

}

func (kv *ShardKV) PollConfig() {
	for {
		time.Sleep(80 * time.Millisecond)
		if _, isLeader := kv.rf.GetState(); !isLeader {
			continue
		}

		newConfig := kv.mck.Query(-1)
		if newConfig.Num < 1 {
			continue
		}
		needShardsAllGrps := map[int]map[int]bool{} // GID->shardSet
		var newShards, rmShards []int
		updated := false
		kv.mu.Lock()
		DPrintf("[KV] oldConf=%+v, newConf=%+v, myShards=%+v\n",
			kv.myConfig.Shards, newConfig.Shards, kv.myShards)
		for shard, gid := range newConfig.Shards {
			if gid == kv.gid {
				if _, ok := kv.myShards[shard]; !ok {
					updated = true
					// Shard newly assigned to my group
					oldGID := kv.myConfig.Shards[shard]
					newShards = append(newShards, shard)
					if _, ok := needShardsAllGrps[oldGID]; !ok {
						needShardsAllGrps[oldGID] = make(map[int]bool)
					}
					needShardsAllGrps[oldGID][shard] = true
				} // Shard taken away from my group: ignore
			} else {
				if _, ok := kv.myShards[shard]; ok {
					updated = true
					// Shard taken away from my group
					delete(kv.myShards, shard)
					rmShards = append(rmShards, shard)
				}
			}
		}

		oldConfig := kv.myConfig
		kv.myConfig = &newConfig
		curNum := newConfig.Num
		kv.mu.Unlock()

		if !updated || oldConfig.Num == 0 {
			// No other shards added
			continue
		}

		DPrintf("[KV] %v-%v receives new config: %+v, needShardsAllGrps:%+v\n",
			kv.gid, kv.me, newConfig, needShardsAllGrps)

		// Get all KVs from other groups
		newKVs := kv.getAllKVsFromOtherGrps(needShardsAllGrps, oldConfig.Groups, curNum)
		op := Op{
			OpType:    RESHARD,
			CliID:     kv.cliID,
			Seq:       kv.seq,
			MovedKVs:  newKVs,
			NewShards: newShards,
			RmShards:  rmShards,
		}

		kv.seq++
		kv.AppendAReShardLog(&op)
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
		myConfig: &shardmaster.Config{},
		cliID:    nrand() % 100000,
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
