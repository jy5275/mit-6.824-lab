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
	STOPOLD = 3
	RESHARD = 4
	NOOP    = 5
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

	// Two logs for each re-sharding: StopOldShards --> ReShard
	// Send FetchShards RPC in phase2
	// Phase1:
	//  1. Start StopOldShards log, wait for apply
	//  2. In applier, remove old shards out of serving set
	// Phase2:
	//  3. Leader FetchShards from other groups (say, G2)
	//     Retry on failure (possibly at this time G2 hasn't known the newest config from shardmaster)
	//     Followers don't send FetchShards RPC. Instead, they just wait for the leader to send new shards
	//     in RESHARD log.
	//  4. Start ReShard log, wait for apply
	//  5. In applier, add new shards into serving set and KVs
	//
	// FetchShards:
	//   1. If args.Num < localNum: discard
	//   2. If STOPOLD log has already been applied at Num: reply with data and return
	//   3. Reject and client retries
	MovedKVs  map[string]string // ReShard
	NewConfig shardmaster.Config
}

func (op *Op) IsEqual(op2 *Op) bool {
	return op.CliID == op2.CliID && op.Seq == op2.Seq
}

type Snapshot struct {
	Data             map[string]string
	NextSeq          map[int64]int
	WorkingConfig    shardmaster.Config
	ConfigOK         bool
	LastIncludedIdx  int
	LastIncludedTerm int
}

type WorkingConfig struct {
	MyShards map[int]bool
	Num      int32
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
	workingConfig     *shardmaster.Config
	shardSuccess      map[int]bool
	cliID             int64
	seq               int
	configOK          bool
}

// Should serve this shard? Invoke with lock!
func (kv *ShardKV) IsResponsibleForShard(shard int) bool {
	if !kv.configOK {
		DPrintf(fmt.Sprintf("[KV] %v-%v is not responsible for shard-%v due to !configOK, workingConfig=%v\n",
			kv.gid, kv.me, shard, kv.workingConfig))
		return false
	}

	return kv.workingConfig.Shards[shard] == kv.gid
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
	e.Encode(kv.workingConfig)
	e.Encode(kv.configOK)
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
	// Decline other shards
	keyShard := key2shard(args.Key)
	if !kv.IsResponsibleForShard(keyShard) {
		reply.Err = ErrWrongGroup
		DPrintf("[KV] %v-%v cannot serve shard %v, args=%v\n",
			kv.gid, kv.me, keyShard, args)
		return
	}
	DPrintf("[KV] %v-%v receives GET cmd %+v, shard=%v\n", kv.gid, kv.me, args, keyShard)

	idx, initTerm, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("[KV] %v-%v is not leader\n", kv.gid, kv.me)
		return
	}

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

		ownThisShard, ok := kv.shardSuccess[idx]
		if ok {
			if !ownThisShard {
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
		}
		DPrintf("[KV] %v-%v is waiting GET log %v to be applied... shardSuccess=%v\n",
			kv.gid, kv.me, args, kv.shardSuccess)

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

	if args.Seq < kv.nextSeq[args.CliID] {
		reply.Err = OK
		DPrintf("[KV] %v-%v gives an early reply to PA cmd %+v, nextSeq=%v\n",
			kv.gid, kv.me, args, kv.nextSeq)
		return
	}

	// Decline other shards
	keyShard := key2shard(args.Key)
	if !kv.IsResponsibleForShard(keyShard) {
		reply.Err = ErrWrongGroup
		DPrintf("[KV] %v-%v cannot serve shard-%v, args=%v\n",
			kv.gid, kv.me, keyShard, args)
		return
	}
	DPrintf("[KV] %v-%v receives PA cmd %+v, shard=%v\n", kv.gid, kv.me, args, keyShard)

	idx, initTerm, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("[KV] %v-%v is not leader\n", kv.gid, kv.me)
		return
	}
	DPrintf("[KV] %v-%v: %+v(idx=%v)\n", kv.gid, kv.me, args, idx)

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
			DPrintf("[KV] %v-%v is not leader, failed to add log %v\n", kv.gid, kv.me, idx)
			return
		}

		ownThisShard, ok := kv.shardSuccess[idx]
		if ok {
			if !ownThisShard {
				reply.Err = ErrWrongGroup
				DPrintf("[KV] %v-%v cannot serve log %v(shard-%v), args=%v, shardSuccess=%v\n",
					kv.gid, kv.me, idx, keyShard, args, kv.shardSuccess)
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
		}
		DPrintf("[KV] %v-%v is waiting PA log %v(idx=%v) to be applied... shardSuccess=%v\n",
			kv.gid, kv.me, args, idx, kv.shardSuccess)

		kv.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
		kv.mu.Lock()
	}
}

func (kv *ShardKV) FetchShards(args *FetchShardsArgs, reply *FetchShardsReply) {
	DPrintf("[KV] %v-%v receives FetchShard req %+v\n", kv.gid, kv.me, args)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("[KV] %v-%v rej FetchShard req due to ErrWrongLeader %+v\n", kv.gid, kv.me, args)
		return
	}

	if args.ConfNum > kv.workingConfig.Num {
		reply.Err = ErrNotFetched
		DPrintf("[KV] %v-%v rej FetchShard req %+v due to config %v is not fetched here, "+
			"workingConfig.Num is %v\n", kv.gid, kv.me, args, args.ConfNum, kv.workingConfig.Num)
		return
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
func (kv *ShardKV) sendFetchShards(toGid int, serverList []string, confNum int, needShards map[int]bool) (map[string]string, Err) {
	args := &FetchShardsArgs{
		NeedShards: needShards,
		ConfNum:    confNum,
	}
	reply := &FetchShardsReply{}

	for leaderID := 0; ; {
		DPrintf("[KV] %v-%v ready to send FetchShard RPC to %v %+v\n",
			kv.gid, kv.me, serverList[leaderID], args)
		ok := kv.make_end(serverList[leaderID]).Call("ShardKV.FetchShards", args, reply)
		DPrintf("[KV] %v-%v receives FetchShard RPC reply from %v-%v: %+v (ok=%v)\n",
			kv.gid, kv.me, toGid, leaderID, reply, ok)

		if ok && reply.Err == OK {
			return reply.MovedKVs, OK
		}

		if !ok {
			// Network error?
			leaderID = (leaderID + 1) % len(serverList)
		} else {
			switch reply.Err {
			case ErrWrongLeader:
				leaderID = (leaderID + 1) % len(serverList)
			case ErrNotFetched:
				//time.Sleep(50 * time.Millisecond)
			default:
				panic(fmt.Sprintf("[KV] %v-%v receives unknown error, reply=%+v\n", kv.gid, kv.me, reply))
				return nil, reply.Err
			}
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
	atomic.StoreInt32(&kv.dead, 1)
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
			ownThisShard := true
			key := applyMsgOp.Key
			sh := key2shard(key)
			val := applyMsgOp.Value
			if applyMsgOp.Seq < kv.nextSeq[applyMsgOp.CliID] {
				kv.shardSuccess[newMsg.CommandIndex] = true
			} else {
				switch applyMsgOp.OpType {
				case GET:
					ownThisShard = kv.IsResponsibleForShard(sh)
					kv.shardSuccess[newMsg.CommandIndex] = ownThisShard
				case PUT:
					ownThisShard = kv.IsResponsibleForShard(sh)
					if ownThisShard {
						kv.data[key] = val
					}
					kv.shardSuccess[newMsg.CommandIndex] = ownThisShard
				case APPEND:
					ownThisShard = kv.IsResponsibleForShard(sh)
					if ownThisShard {
						kv.data[key] += val
					}
					kv.shardSuccess[newMsg.CommandIndex] = ownThisShard
					DPrintf("[KV] %v-%v append result <%v, %v>\n", kv.gid, kv.me, key, kv.data[key])
				case STOPOLD:
					if applyMsgOp.NewConfig.Num < kv.workingConfig.Num {
						//panic(fmt.Sprintf("[KV] %v-%v applies a outdated config log! msg=%+v, workingConfig=%+v\n",
						//	kv.gid, kv.me, applyMsgOp, kv.workingConfig))
					}
					if applyMsgOp.NewConfig.Num > kv.workingConfig.Num {
						// the newest config is new to us
						kv.configOK = false
						kv.workingConfig = &applyMsgOp.NewConfig
					}

				case RESHARD:
					DPrintf("[KV] %v-%v append type-4 log, configOK=%v, working=%v, msg=%v\n",
						kv.gid, kv.me, kv.configOK, kv.workingConfig, applyMsgOp)
					if applyMsgOp.NewConfig.Num == kv.workingConfig.Num && !kv.configOK {
						for k, v := range applyMsgOp.MovedKVs {
							kv.data[k] = v
						}
						kv.configOK = true
					}

				case NOOP:
				// Do nothing...
				default:
					panic(fmt.Sprintf("[KV] %v-%v applied a invalid log type(%T), %+v\n", kv.gid, kv.me, applyMsgOp, applyMsgOp))
				}

				// If ErrWrongGroup, this log doesn't have any effect, so seq num shouldn't increase
				if ownThisShard {
					kv.nextSeq[applyMsgOp.CliID] = applyMsgOp.Seq + 1
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
			if d.Decode(&snapMsg.WorkingConfig) != nil {
				panic("Decode WorkingConfig error")
			}
			if d.Decode(&snapMsg.ConfigOK) != nil {
				panic("Decode ConfigOK error")
			}
			if d.Decode(&snapMsg.LastIncludedIdx) != nil {
				panic("Decode lastIncludedIdx error")
			}

			kv.data = snapMsg.Data
			kv.nextSeq = snapMsg.NextSeq
			kv.workingConfig = &snapMsg.WorkingConfig
			kv.configOK = snapMsg.ConfigOK
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
			kv.DoSnapshot() // 等待 AppendLogFromKVServer witness 后退出，但后者在等待他 snapshot 完成???
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) getAllKVsFromOtherGrps(needShardsAllGrps map[int]map[int]bool, oldGrps map[int][]string,
	curNum int) (map[string]string, []Err) {
	newKVs := map[string]string{}
	type FetchKVsResult struct {
		NewKVs map[string]string
		Err    Err
	}
	newKVsCh := make(chan FetchKVsResult, 10)

	for oldGID, shardSet := range needShardsAllGrps {
		serverList, ok := oldGrps[oldGID]
		if !ok {
			panic(fmt.Sprintf("[KV] %v-%v requests shards from a non-existing server! "+
				"needShardsAllGrps=%+v, oldGrp=%+v, curNum=%v\n",
				kv.gid, kv.me, needShardsAllGrps, oldGrps, curNum))
		}
		shardSetCache := shardSet
		go func(oldGID int) {
			fetchedKVs, errMsg := kv.sendFetchShards(oldGID, serverList, curNum, shardSetCache)
			newKVsCh <- FetchKVsResult{
				NewKVs: fetchedKVs,
				Err:    errMsg,
			}
		}(oldGID)
	}

	var errors []Err
	for i := 0; i < len(needShardsAllGrps); i++ {
		result := <-newKVsCh
		if result.Err != OK {
			errors = append(errors, result.Err)
		}

		if result.NewKVs == nil {
			continue
		}
		for k, v := range result.NewKVs {
			newKVs[k] = v
		}
	}

	return newKVs, errors
}

// Should invoke with lock!
func (kv *ShardKV) AppendLogFromKVServer(op *Op) (ret bool) {
	idx, initTerm, isLeader := kv.rf.Start(*op)
	DPrintf("[KV] %v-%v tries to start a KV-level log: %+v, idx=%v\n",
		kv.gid, kv.me, op, idx)

	if !isLeader {
		DPrintf("[KV] %v-%v is not leader, discard KV-level log: %+v\n",
			kv.gid, kv.me, op)
		// Do nothing...
		return ret
	}

	kv.sleepCnt.Add(idx)
	defer func() {
		kv.sleepCnt.Sub(idx)
		kv.cond.Broadcast()
	}()

	for !kv.killed() {
		curTerm, isLeader := kv.rf.GetState()
		if !isLeader || curTerm != initTerm {
			return false
		}

		fetchedLog := kv.rf.FetchLogContent(idx)
		if kv.lastAppliedIndex >= idx && fetchedLog != nil {
			if realLog, ok := fetchedLog.(Op); ok {
				DPrintf("[KV] %v-%v has witnessed the KV-level log at idx=%v: %+v\n",
					kv.gid, kv.me, idx, realLog)
				if realLog.CliID == op.CliID && realLog.Seq == op.Seq { // Success
					ret = true
					kv.seq++
				} else { // Fail
					ret = false
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

	return ret
}

func (kv *ShardKV) AppendTwoLogs(newConfig *shardmaster.Config, newLeader bool) {
	kv.mu.Lock()
	if newConfig.Num < 1 || (newConfig.Num == kv.workingConfig.Num && kv.configOK) {
		kv.mu.Unlock()
		return
	}

	if newLeader {
		newLeaderNoop := Op{
			OpType: NOOP,
		}
		kv.AppendLogFromKVServer(&newLeaderNoop)
	}

	if !kv.configOK {
		// need repair: should fetch the (working.Num-1)th config, discard newConfig...
		DPrintf("[KV] %v-%v starts repairing workingConfig:%v\n", kv.gid, kv.me, kv.workingConfig)

		oldWorkingConfig := kv.workingConfig
		if newConfig.Num != oldWorkingConfig.Num-1 {
			kv.mu.Unlock()
			prevConfig := kv.mck.Query(kv.workingConfig.Num - 1)
			kv.mu.Lock()

			oldGroupServers := prevConfig.Groups
			needShardsAllGrps := map[int]map[int]bool{} // GID->shardSet
			for shard, gid := range kv.workingConfig.Shards {
				if gid == kv.gid {
					if prevConfig.Shards[shard] != kv.gid {
						oldGID := prevConfig.Shards[shard]
						if oldGID == 0 { // 0 means this shard is not allocated
							continue
						}
						if _, ok := needShardsAllGrps[oldGID]; !ok {
							needShardsAllGrps[oldGID] = make(map[int]bool)
						}
						needShardsAllGrps[oldGID][shard] = true // need to fetch `shard` from `oldGID`
					}
				}
			}

			if kv.workingConfig.Num != oldWorkingConfig.Num {
				DPrintf("[KV] %v-%v failed to repair due to already updated after shard query RPC, "+
					"configOK=%v, oldWorkingConfig=%+v, workingConfig=%v\n",
					kv.gid, kv.me, kv.configOK, oldWorkingConfig, kv.workingConfig)
				kv.mu.Unlock()
				return
			}

			DPrintf("[KV] %v-%v is repairing workingConfig:%+v, prevConfig=%+v, needShardsAllGrps=%v\n",
				kv.gid, kv.me, kv.workingConfig, prevConfig, needShardsAllGrps)

			kv.mu.Unlock()
			newKVs, errors := kv.getAllKVsFromOtherGrps(needShardsAllGrps, oldGroupServers, oldWorkingConfig.Num)
			if len(errors) > 0 {
				// Might due to outdated config num, just return and get the newest config
				DPrintf("[KV] %v-%v failed to fetch shards from other groups, err=%+v\n",
					kv.gid, kv.me, errors)
				return
			}

			kv.mu.Lock()
			if kv.workingConfig.Num != oldWorkingConfig.Num {
				DPrintf("[KV] %v-%v failed to repair due to already updated after FetchShards, "+
					"configOK=%v, oldWorkingConfig=%+v, workingConfig=%v\n",
					kv.gid, kv.me, kv.configOK, oldWorkingConfig, kv.workingConfig)
				kv.mu.Unlock()
				return
			}

			op2 := Op{
				OpType:   RESHARD,
				CliID:    kv.cliID,
				Seq:      kv.seq,
				MovedKVs: newKVs,
				NewConfig: shardmaster.Config{
					Num: oldWorkingConfig.Num,
				},
			}
			kv.AppendLogFromKVServer(&op2)
			DPrintf("[KV] %v-%v has repaired config %+v\n", kv.gid, kv.me, oldWorkingConfig)
			kv.mu.Unlock()

			return
		}
	}

	oldGroupServers := kv.workingConfig.Groups
	needShardsAllGrps := map[int]map[int]bool{} // GID->shardSet
	for shard, gid := range newConfig.Shards {
		if gid == kv.gid {
			if kv.workingConfig.Shards[shard] != kv.gid {
				oldGID := kv.workingConfig.Shards[shard]
				if oldGID == 0 { // 0 means this shard is not allocated
					continue
				}
				if _, ok := needShardsAllGrps[oldGID]; !ok {
					needShardsAllGrps[oldGID] = make(map[int]bool)
				}
				needShardsAllGrps[oldGID][shard] = true // need to fetch `shard` from `oldGID`
			}
		}
	}

	DPrintf("[KV] %v-%v receives new config: %+v, workingConfig:%+v, needShardsAllGrps=%v\n",
		kv.gid, kv.me, newConfig, kv.workingConfig, needShardsAllGrps)

	op1 := Op{
		OpType:    STOPOLD,
		CliID:     kv.cliID,
		Seq:       kv.seq,
		NewConfig: *newConfig,
	}
	if logSucceed := kv.AppendLogFromKVServer(&op1); !logSucceed {
		// May fail due to lose leadership...
		DPrintf("[KV] %v-%v append STOPOLD log failed %+v\n", kv.gid, kv.me, op1)
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()

	newKVs, errors := kv.getAllKVsFromOtherGrps(needShardsAllGrps, oldGroupServers, newConfig.Num)
	if len(errors) > 0 {
		// Might due to outdated config num, just return and get the newest config
		DPrintf("[KV] %v-%v failed to fetch shards from other groups, err=%+v\n",
			kv.gid, kv.me, errors)
		return
	}

	op2 := Op{
		OpType:   RESHARD,
		CliID:    kv.cliID,
		Seq:      kv.seq,
		MovedKVs: newKVs,
		NewConfig: shardmaster.Config{
			Num: newConfig.Num,
		},
	}
	kv.mu.Lock()
	kv.AppendLogFromKVServer(&op2)
	DPrintf("[KV] %v-%v has shift to new config %+v\n", kv.gid, kv.me, newConfig)
	kv.mu.Unlock()
}

func (kv *ShardKV) PollConfig() {
	lastTerm := 0
	for !kv.killed() {
		time.Sleep(100 * time.Millisecond)
		ImNewLeader := false
		term, isLeader := kv.rf.GetState()
		if !isLeader {
			continue
		}

		if lastTerm != term {
			ImNewLeader = true
			lastTerm = term
		}

		nextConfigID := kv.workingConfig.Num + 1
		if nextConfigID == 1 {
			nextConfigID = -1
		}
		newConfig := kv.mck.Query(nextConfigID)

		kv.mu.Lock()
		DPrintf(fmt.Sprintf("[KV] %v-%v has pulled a newConfig=%+v, configOK=%v, workingConfig=%+v",
			kv.gid, kv.me, newConfig, kv.configOK, kv.workingConfig))
		kv.mu.Unlock()

		// after failure recovery out KV might have workingConf.Num < cacheConf.Num,
		// in this case we should still send FetchShards RPC.
		if newConfig.Num < kv.workingConfig.Num {
			panic(fmt.Sprintf("[KV] %v-%v new config num(%v) should less than cur config num(%v)!\n",
				kv.gid, kv.me, newConfig.Num, kv.workingConfig.Num))
		}

		// Get all KVs from other groups
		kv.AppendTwoLogs(&newConfig, ImNewLeader)
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
		mck:           shardmaster.MakeClerk(masters),
		workingConfig: &shardmaster.Config{},
		shardSuccess:  make(map[int]bool),
		cliID:         nrand() % 100000,
		configOK:      true,
	}

	// Your initialization code here.
	kv.cond = sync.NewCond(&kv.mu)
	kv.rf = raft.MakeWithDebug(servers, me, persister, kv.applyCh, true, gid)

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
		if d.Decode(&kv.workingConfig) != nil {
			panic("Decode workingConfig error")
		}
		if d.Decode(&kv.configOK) != nil {
			panic("Decode configOK error")
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

	DPrintf("Server %v-%v started with maxraftstate=%v, lastAppliedIdx=%v, "+
		"snapshot's lastIncludedIdx=%v\n",
		kv.gid, kv.me, maxraftstate, kv.lastAppliedIndex, lastIncludedIdx)
	go kv.ApplyChListener()
	go kv.PollConfig()

	return kv
}
