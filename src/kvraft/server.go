package kvraft

import (
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Opid     int
	Client   int64
	Key      string
	Value    string
	Command  string
	WaitCh   chan AppliedOp
	LastSeen int
}

type AppliedOp struct {
	Success bool
	Index   int
	Term    int
	Command string
}

type recordRes struct {
	err   Err
	value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data   map[string]string
	record map[int64]map[int]recordRes
}

func (kv *KVServer) waitApply(idx int, term int, command string, uniID int, key string, waitCh chan AppliedOp) (bool, bool) {
	ticker := time.NewTicker(500 * time.Millisecond)
	select {
	case doneOp := <-waitCh:
		DPrintf("start returned for uniID op %v: [%v, %v, %v]; commiter returned: [%v, %v, %v]", uniID, idx, term, command, doneOp.Index, doneOp.Term, doneOp.Command)
		if doneOp.Index != idx || doneOp.Term != term || doneOp.Command != command {
			return false, false
		}
		ticker.Stop()
		return true, false
	case <-ticker.C:
		ticker.Stop()
		return false, true
	}
}

func prepareOp(opid int, client int64, key, value, optype string, waitRaftCh chan AppliedOp, lastseen int) Op {
	op := Op{
		Opid:     opid,
		Client:   client,
		Key:      key,
		Value:    value,
		Command:  optype,
		WaitCh:   waitRaftCh,
		LastSeen: lastseen,
	}
	return op
}

// Get RPC handler
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	if _, ok := kv.record[args.Client]; !ok {
		kv.record[args.Client] = make(map[int]recordRes)
	}
	clientMap := kv.record[args.Client]
	// check if the command is a duplicated command that has already been applied
	if val, ok := clientMap[args.Opid]; ok {
		reply.Err = val.err
		reply.Value = val.value
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// it is a new command so put it into raft log
	waitRaftCh := make(chan AppliedOp)
	op := prepareOp(args.Opid, args.Client, args.Key, "", "Get", waitRaftCh, args.LastSeenOpid)

	idx, term, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.Value = ""
		return
	}

	if success, timeout := kv.waitApply(idx, term, "Get", args.Opid, args.Key, waitRaftCh); success {
		reply.Err = ""
		kv.mu.Lock()
		reply.Value = kv.data[args.Key]
		kv.mu.Unlock()
	} else {
		if timeout {
			reply.Err = ErrRaftNoRes
		} else {
			reply.Err = ErrWrongLeader
		}
		reply.Value = ""
	}

	kv.mu.Lock()
	DPrintf("Server %v finish GET op for client %v: uniID: %v, key: %v, get value: %v\n", kv.me, args.Client, args.Opid, args.Key, kv.data[args.Key])
	kv.mu.Unlock()
}

// PutAppend RPC handler
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	if _, ok := kv.record[args.Client]; !ok {
		kv.record[args.Client] = make(map[int]recordRes)
	}
	clientMap := kv.record[args.Client]
	//check if the command is a duplicated command that has already been applied
	if val, ok := clientMap[args.Opid]; ok {
		reply.Err = val.err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// it is a new command so put it into raft log
	waitRaftCh := make(chan AppliedOp, 1)
	op := prepareOp(args.Opid, args.Client, args.Key, args.Value, args.Op, waitRaftCh, args.LastSeenOpid)

	idx, term, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	if success, timeout := kv.waitApply(idx, term, args.Op, args.Opid, args.Key, waitRaftCh); success {
		reply.Err = ""
	} else {
		if timeout {
			reply.Err = ErrRaftNoRes
		} else {
			reply.Err = ErrWrongLeader
		}
	}
	kv.mu.Lock()
	DPrintf("Server %v finish putAppend op for client %v: uniID: %v, key: %v, value: %v, type: %v, curr value: %v\n", kv.me, args.Client, args.Opid, args.Key, args.Value, args.Op, kv.data[args.Key])
	kv.mu.Unlock()
}

func (kv *KVServer) updateHistoryMap(client int64, opid, lastSeen int, val string) {
	clientMap := kv.record[client]
	// Last seen client request can be deleted from the record map to free memory
	delete(clientMap, lastSeen)
	// Record the execute results into the client map
	clientMap[opid] = recordRes{
		err:   "",
		value: val,
	}
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		select {
		case op := <-kv.applyCh:
			DPrintf("server %v receives a applymsg from ch: %v\n", kv.me, op)
			if op.CommandValid {
				success := false
				replyValue := ""
				applyOp := op.Command.(Op)

				// it is possible that the client history map has not been initialized yet here if it is on follower
				// so need to check if map exists here. If not, create the client history map on follower's state machine
				kv.mu.Lock()
				if _, ok := kv.record[applyOp.Client]; !ok {
					kv.record[applyOp.Client] = make(map[int]recordRes)
				}
				clientMap := kv.record[applyOp.Client]

				// it is possible that a command shown here is a duplicated OP and has already been executed
				if _, ok := clientMap[applyOp.Opid]; ok {
					kv.mu.Unlock()
					continue
				}

				if applyOp.Command == "Put" {
					kv.data[applyOp.Key] = applyOp.Value

				} else if applyOp.Command == "Append" {
					if _, ok := kv.data[applyOp.Key]; !ok {
						kv.data[applyOp.Key] = applyOp.Value
					} else {
						ss := []string{kv.data[applyOp.Key], applyOp.Value}
						kv.data[applyOp.Key] = strings.Join(ss, "")
					}

				} else if applyOp.Command == "Get" {
					if _, ok := kv.data[applyOp.Key]; !ok {
						replyValue = ""
					} else {
						replyValue = kv.data[applyOp.Key]
					}
				}
				kv.updateHistoryMap(applyOp.Client, applyOp.Opid, applyOp.LastSeen, replyValue)
				success = true
				kv.mu.Unlock()

				if applyOp.WaitCh == nil {
					continue
				}

				replyMsg := AppliedOp{
					Success: success,
					Index:   op.CommandIndex,
					Term:    op.CommandTerm,
					Command: applyOp.Command,
				}
				applyOp.WaitCh <- replyMsg
			}
		}

	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.record = make(map[int64]map[int]recordRes)

	go kv.applier()

	return kv
}
