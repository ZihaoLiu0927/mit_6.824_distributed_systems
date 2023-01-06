package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	nServer      int
	opid         int
	lastLeader   int
	clientId     int64
	lastSeenOpid int
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
	// You'll have to add code here.
	ck.nServer = len(ck.servers)
	ck.opid = 0
	ck.lastSeenOpid = -1
	ck.lastLeader = int(nrand()) % ck.nServer
	ck.clientId = nrand()
	return ck
}

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
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:          key,
		Opid:         ck.opid,
		Client:       ck.clientId,
		LastSeenOpid: ck.lastSeenOpid,
	}
	ck.opid += 1

	i := ck.lastLeader
	for {
		DPrintf("Client %v tries to connect server %v out of %v server for op %v\n", ck.clientId, i, ck.nServer, args.Opid)
		reply := GetReply{}
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)

		if !ok || reply.Err != "" {
			i = (i + 1) % ck.nServer
			continue
		}

		ck.lastLeader = i
		ck.lastSeenOpid = args.Opid
		return reply.Value
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Op:           op,
		Key:          key,
		Value:        value,
		Opid:         ck.opid,
		Client:       ck.clientId,
		LastSeenOpid: ck.lastSeenOpid,
	}
	ck.opid += 1

	i := ck.lastLeader
	for {
		DPrintf("Client %v tries to connect server %v out of %v server \n", ck.clientId, i, ck.nServer)
		reply := PutAppendReply{}
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)

		if !ok || reply.Err != "" {
			i = (i + 1) % ck.nServer
			continue
		}

		ck.lastLeader = i
		ck.lastSeenOpid = args.Opid
		return
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
