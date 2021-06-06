package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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
	ck.cliID = nrand() % 100000
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	reply := &GetReply{}
	args := &GetArgs{
		Seq:   ck.seq,
		Key:   key,
		CliID: ck.cliID,
	}
	leaderId := ck.cachedLeader
	for {
		DPrintf("Cli %v send GET cmd{k=%v} to server %v\n",
			ck.cliID, key, leaderId)
		ok := ck.servers[leaderId].Call("KVServer.Get", args, reply)
		if ok && (reply.Err == "" || reply.Err == OK || reply.Err == ErrNoKey) {
			ck.cachedLeader = leaderId
			DPrintf("Cli %v send GET cmd{k=%v} to server %v success, err=%v\n",
				ck.cliID, key, leaderId, reply.Err)
			break
		}
		leaderId = (leaderId + 1) % len(ck.servers)
		time.Sleep(50 * time.Millisecond)
	}
	DPrintf("Cli %v get data: <%v, %v> ok\n", ck.cliID, key, reply.Value)
	ck.seq++
	return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	reply := &PutAppendReply{}
	args := &PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		CliID: ck.cliID,
		Seq:   ck.seq,
	}
	leaderId := ck.cachedLeader
	for {
		DPrintf("Cli %v send PA cmd{op=%v, k=%v, v=%v} to server %v\n",
			ck.cliID, op, key, value, leaderId)
		ok := ck.servers[leaderId].Call("KVServer.PutAppend", args, reply)
		if ok && (reply.Err == "" || reply.Err == OK || reply.Err == ErrNoKey) {
			ck.cachedLeader = leaderId
			DPrintf("Cli %v send PA cmd{op=%v, k=%v, v=%v} to server %v success, err=%v\n",
				ck.cliID, op, key, value, leaderId, reply.Err)
			break
		}
		leaderId = (leaderId + 1) % len(ck.servers)
		time.Sleep(50 * time.Millisecond)
	}
	ck.seq++
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
	DPrintf("Cli %v put data: <%v, %v> ok\n", ck.cliID, key, value)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
	DPrintf("Cli %v append data: <%v, ...%v> ok\n", ck.cliID, key, value)
}
