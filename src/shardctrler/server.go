package shardctrler

import (
	"sync"

	"time"

	"log"

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

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num
	record  map[int64]map[int]recordRes
	timeout time.Duration
}

type recordRes struct {
	Err   Err
	Value string
}

type Op struct {
	// Your data here.
	Opid     int
	Client   int64
	LastSeen int

	Type        string
	CommandArgs OpArgs
}

type OpArgs struct {
	JoinArgs  map[int][]string
	LeaveArgs []int
	MoveArgs  []int
	QueryArgs int
}

type AppliedOp struct {
	Success bool
	Index   int
	Term    int
	Command string
}

func prepareOp(opid int, client int64, waitRaftCh chan AppliedOp, lastseen int, optype string, args OpArgs) Op {
	op := Op{
		Opid:        opid,
		Client:      client,
		LastSeen:    lastseen,
		CommandArgs: args,
	}
	return op
}

func (sc *ShardCtrler) waitApply(idx int, term int, uniID int, waitCh chan AppliedOp) (bool, bool) {
	ticker := time.NewTicker(sc.timeout)
	select {
	case doneOp := <-waitCh:
		DPrintf("start returned for uniID op %v: [%v, %v]; commiter returned: [%v, %v]", uniID, idx, term, doneOp.Index, doneOp.Term)
		if doneOp.Index != idx || doneOp.Term != term {
			return false, false
		}
		ticker.Stop()
		return true, false
	case <-ticker.C:
		ticker.Stop()
		return false, true
	}
}

func (sc *ShardCtrler) isOldRequest(client int64, opid int) (bool, recordRes) {
	sc.mu.Lock()
	if _, ok := sc.record[client]; !ok {
		sc.record[client] = make(map[int]recordRes)
	}
	clientMap := sc.record[client]
	// check if the command is a duplicated command that has already been applied
	if val, ok := clientMap[opid]; ok {
		sc.mu.Unlock()
		return true, val
	}
	sc.mu.Unlock()
	return false, recordRes{}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	if oldReq, val := sc.isOldRequest(args.Client, args.Opid); oldReq {
		reply.Err = val.Err
		return
	}

	// it is a new command so put it into raft log
	waitRaftCh := make(chan AppliedOp)
	op := prepareOp(args.Opid, args.Client, waitRaftCh, args.LastSeenOpid, JOIN, OpArgs{JoinArgs: args.Servers})

	idx, term, isLeader := sc.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	if success, timeout := sc.waitApply(idx, term, args.Opid, waitRaftCh); !success {
		if timeout {
			reply.Err = ErrRaftNoRes
		} else {
			reply.WrongLeader = true
		}
	}

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	if oldReq, val := sc.isOldRequest(args.Client, args.Opid); oldReq {
		reply.Err = val.Err
		return
	}

	// it is a new command so put it into raft log
	waitRaftCh := make(chan AppliedOp)
	op := prepareOp(args.Opid, args.Client, waitRaftCh, args.LastSeenOpid)

	idx, term, isLeader := sc.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	if success, timeout := sc.waitApply(idx, term, args.Opid, waitRaftCh); !success {
		if timeout {
			reply.Err = ErrRaftNoRes
		} else {
			reply.WrongLeader = true
		}
	}

}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	if oldReq, val := sc.isOldRequest(args.Client, args.Opid); oldReq {
		reply.Err = val.Err
		return
	}

	// it is a new command so put it into raft log
	waitRaftCh := make(chan AppliedOp)
	op := prepareOp(args.Opid, args.Client, waitRaftCh, args.LastSeenOpid)

	idx, term, isLeader := sc.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	if success, timeout := sc.waitApply(idx, term, args.Opid, waitRaftCh); !success {
		if timeout {
			reply.Err = ErrRaftNoRes
		} else {
			reply.WrongLeader = true
		}
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	if oldReq, val := sc.isOldRequest(args.Client, args.Opid); oldReq {
		reply.Err = val.Err
		return
	}

	// it is a new command so put it into raft log
	waitRaftCh := make(chan AppliedOp)
	op := prepareOp(args.Opid, args.Client, waitRaftCh, args.LastSeenOpid)

	idx, term, isLeader := sc.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	if success, timeout := sc.waitApply(idx, term, args.Opid, waitRaftCh); !success {
		if timeout {
			reply.Err = ErrRaftNoRes
		} else {
			reply.WrongLeader = true
		}
	}
}

func (sc *ShardCtrler) applier() {
	// for {
	// 	select {
	// 	case op := <-sc.applyCh:
	// 	}
	// }
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.record = make(map[int64]map[int]recordRes)
	sc.timeout = 500 * time.Millisecond

	return sc
}
