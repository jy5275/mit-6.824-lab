package shardmaster

//
// Shardmaster clerk.
//

import "../labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	cachedLeader int
	cliID        int64
	seq          int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.cliID = nrand() % 100000
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{
		BasicArgs: BasicArgs{
			Seq:   ck.seq,
			CliID: ck.cliID,
		},
		Num: num,
	}
	leaderID := ck.cachedLeader

	// Your code here.
	var reply QueryReply
	// try each known server.
	for {
		DPrintf("Cli %v send Query cmd{num=%v} to server %v\n",
			ck.cliID, num, leaderID)
		ok := ck.servers[leaderID].Call("ShardMaster.Query", args, &reply)
		DPrintf("Cli %v recv resp of Query cmd{num=%v} from server %v, resp=%+v\n", ck.cliID, num, leaderID, reply)
		if ok && reply.Err == OK {
			ck.cachedLeader = leaderID
			DPrintf("Cli %v send Query cmd{num=%v} to server %v success, err=%v\n",
				ck.cliID, num, leaderID, reply.Err)
			break
		}
		leaderID = (leaderID + 1) % len(ck.servers)
		time.Sleep(50 * time.Millisecond)
	}
	DPrintf("Cli %v Query: <%v, %+v> ok\n", ck.cliID, num, reply.Config)
	ck.seq++
	return reply.Config
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{
		BasicArgs: BasicArgs{
			Seq:   ck.seq,
			CliID: ck.cliID,
		},
		Servers: servers,
	}
	leaderID := ck.cachedLeader

	// Your code here.
	var reply JoinReply
	// try each known server.
	for {
		DPrintf("Cli %v send Join cmd{servers=%+v} to server %v\n",
			ck.cliID, servers, leaderID)
		ok := ck.servers[leaderID].Call("ShardMaster.Join", args, &reply)
		if ok && reply.Err == OK {
			ck.cachedLeader = leaderID
			DPrintf("Cli %v send Join cmd{servers=%+v} to server %v success, err=%v\n",
				ck.cliID, servers, leaderID, reply.Err)
			break
		}
		leaderID = (leaderID + 1) % len(ck.servers)
		time.Sleep(50 * time.Millisecond)
	}
	DPrintf("Cli %v Join: <%+v> ok\n", ck.cliID, servers)
	ck.seq++
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{
		BasicArgs: BasicArgs{
			Seq:   ck.seq,
			CliID: ck.cliID,
		},
		GIDs: gids,
	}
	leaderID := ck.cachedLeader

	// Your code here.
	var reply LeaveReply
	for {
		DPrintf("Cli %v send Leave cmd{gids=%+v} to server %v\n",
			ck.cliID, gids, leaderID)
		ok := ck.servers[leaderID].Call("ShardMaster.Leave", args, &reply)
		if ok && reply.Err == OK {
			ck.cachedLeader = leaderID
			DPrintf("Cli %v send Leave cmd{gids=%+v} to server %v success, err=%v\n",
				ck.cliID, gids, leaderID, reply.Err)
			break
		}
		leaderID = (leaderID + 1) % len(ck.servers)
		time.Sleep(50 * time.Millisecond)
	}
	DPrintf("Cli %v Leave: <%+v> ok\n", ck.cliID, gids)
	ck.seq++
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{
		BasicArgs: BasicArgs{
			Seq:   ck.seq,
			CliID: ck.cliID,
		},
		Shard: shard,
		GID:   gid,
	}
	leaderID := ck.cachedLeader
	// Your code here.
	var reply MoveReply
	for {
		DPrintf("Cli %v send Move cmd{shard=%v, gid=%v} to server %v\n",
			ck.cliID, shard, gid, leaderID)
		ok := ck.servers[leaderID].Call("ShardMaster.Move", args, &reply)
		if ok && reply.Err == OK {
			ck.cachedLeader = leaderID
			DPrintf("Cli %v send Move cmd{shard=%v, gid=%v} to server %v success, err=%v\n",
				ck.cliID, shard, gid, leaderID, reply.Err)
			break
		}
		leaderID = (leaderID + 1) % len(ck.servers)
		time.Sleep(50 * time.Millisecond)
	}
	DPrintf("Cli %v Move: shard:%v, gid:%v ok\n", ck.cliID, shard, gid)
	ck.seq++
}
