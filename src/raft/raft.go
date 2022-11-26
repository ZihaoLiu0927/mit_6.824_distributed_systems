package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"sync"
	"sync/atomic"

	//	"6.824/labgob"

	"math/rand"
	"sort"
	"time"

	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Log struct {
	Command     interface{}
	TermReceive int
	Pos         int
}

type LeaderState struct {
	nextIndex  []int
	matchIndex []int
}

type State struct {
	currentTerm int
	votedFor    int
	logs        []Log
	commitIndex int
	lastApplied int
	leaderId    int
	leaderState LeaderState
}

const (
	Leader    Status = "leader"
	Candidate Status = "candidate"
	Follower  Status = "follower"
)

type Status string

// A Go object implementing a single Raft peer.
type Raft struct {
	mu            sync.Mutex          // Lock to protect shared access to this peer's state
	peers         []*labrpc.ClientEnd // RPC end points of all peers
	persister     *Persister          // Object to hold this peer's persisted state
	me            int                 // this peer's index into peers[]
	dead          int32               // set by Kill()
	status        State
	lastHeartbeat time.Time
	timeout       int
	state         Status
	applyCh       chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func (this Log) equal(another Log) bool {
	if this.TermReceive == another.TermReceive && this.Pos == another.Pos {
		return true
	}
	return false
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.status.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.status.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.status.currentTerm || rf.killed() {
		return
	}

	if args.Term > rf.status.currentTerm {
		rf.state = Follower
		rf.status.currentTerm = args.Term
		rf.status.votedFor = -1
		rf.status.leaderId = -1
	}

	// Election restriction in paper 5.4.1:
	// the voter denies its vote if its own log is more up-to-date than that of the candidate
	if rf.status.votedFor == -1 || rf.status.votedFor == args.CandidateId {

		myLastLogTerm := 0
		if len(rf.status.logs) > 0 {
			myLastLogTerm = rf.status.logs[len(rf.status.logs)-1].TermReceive
		}

		// compare their terms first
		if args.LastLogTerm > myLastLogTerm {
			reply.VoteGranted = true
			rf.status.votedFor = args.CandidateId
			rf.resetTimer()
			DPrintf("server %v last log[%v, %v] grants candidate %v last log[%v, %v] bc newer term\n", rf.me, rf.status.logs[len(rf.status.logs)-1].Pos, rf.status.logs[len(rf.status.logs)-1].TermReceive, args.CandidateId, args.LastLogIndex, args.LastLogTerm)
			return
		}

		// if tied on the term, then compare log length
		if args.LastLogTerm == myLastLogTerm && len(rf.status.logs)-1 <= args.LastLogIndex {
			reply.VoteGranted = true
			rf.status.votedFor = args.CandidateId
			rf.resetTimer()
			DPrintf("server %v last log[%v, %v] grants candidate %v last log[%v, %v] bc longer log length\n", rf.me, rf.status.logs[len(rf.status.logs)-1].Pos, rf.status.logs[len(rf.status.logs)-1].TermReceive, args.CandidateId, args.LastLogIndex, args.LastLogTerm)
			return
		}

		//DPrintf("arg last term: %v, my last term %v; ")
	}

}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []Log
}

type AppendEntriesReply struct {
	Term        int
	Success     bool
	BackupIndex int
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.status.currentTerm
	reply.Success = false
	reply.BackupIndex = -1

	// step 1:  Reply false if term < currentTerm
	if args.Term < rf.status.currentTerm || rf.killed() {
		return
	}

	if args.Term > rf.status.currentTerm {
		rf.status.currentTerm = args.Term
		rf.status.votedFor = -1
		rf.state = Follower
		rf.status.leaderId = -1
	}

	rf.status.leaderId = args.LeaderId

	DPrintf("Heartbeat for %v[%v], from leader %v[%v], current logs is: [pos: %v, term: %v]", rf.me, rf.status.currentTerm, args.LeaderId, args.Term, rf.status.logs[len(rf.status.logs)-1].Pos, rf.status.logs[len(rf.status.logs)-1].TermReceive)

	rf.resetTimer()

	// step 2: Reply false if log doesn’t contain an entry at prevLogIndex
	if args.PrevLogIndex >= 0 && len(rf.status.logs) < args.PrevLogIndex+1 {
		reply.BackupIndex = len(rf.status.logs) - 1
		return
	}

	//step 2: Reply false if log does contain an entry at prevLogIndex but term does not matche prevLogTerm
	if args.PrevLogIndex >= 0 && rf.status.logs[args.PrevLogIndex].TermReceive != args.PrevLogTerm {
		i := args.PrevLogIndex
		badTerm := rf.status.logs[args.PrevLogIndex].TermReceive
		for ; rf.status.logs[i].TermReceive == badTerm; i-- {
		}
		reply.BackupIndex = i + 1
		return
	}

	// step 3 and 4: append all new entries,
	// really need to check -1 here?
	curr := args.PrevLogIndex + 1
	count := 0
	for _, entry := range args.Entries {
		if curr == len(rf.status.logs) {
			break
		}
		if !rf.status.logs[curr].equal(entry) {
			rf.status.logs = rf.status.logs[0:curr]
			break
		}
		curr += 1
		count += 1
	}
	if count < len(args.Entries) {
		rf.status.logs = append(rf.status.logs, args.Entries[count:]...)
	}

	// step 5: leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.status.commitIndex && len(args.Entries) > 0 {
		rf.status.commitIndex = min(args.LeaderCommit, args.Entries[len(args.Entries)-1].Pos)

	} else if args.LeaderCommit > rf.status.commitIndex && len(args.Entries) == 0 {
		rf.status.commitIndex = min(args.LeaderCommit, args.PrevLogIndex)
	}

	reply.Success = true
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	peer := rf.peers[server]
	return peer.Call("Raft.RequestVote", args, reply)
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term, isLeader = rf.status.currentTerm, rf.state == Leader

	if !isLeader || rf.killed() {
		return index, term, isLeader
	}

	index = rf.status.leaderState.nextIndex[rf.me]
	rf.status.logs = append(rf.status.logs,
		Log{
			Command:     command,
			TermReceive: rf.status.currentTerm,
			Pos:         index,
		})
	rf.status.leaderState.nextIndex[rf.me] += 1
	rf.status.leaderState.matchIndex[rf.me] = index

	DPrintf("New command come to leader %v[%v], command position: %v", rf.me, rf.status.currentTerm, index)
	DPrintf("Leader %v[%v] current logs is: [pos: %v, term: %v]; current commitIndex is: %v\n", rf.me, rf.status.currentTerm, rf.status.logs[len(rf.status.logs)-1].Pos, rf.status.logs[len(rf.status.logs)-1].TermReceive, rf.status.commitIndex)
	// apply this to state machine not implemented
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) generateTimeOut() int {
	return 200 + rand.Intn(150)
}

func (rf *Raft) resetTimer() {
	rf.timeout = rf.generateTimeOut()
	rf.lastHeartbeat = time.Now()
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		rf.mu.Lock()
		lastHeartbeat := rf.lastHeartbeat
		timeout := rf.timeout
		isLeader := rf.state == Leader
		rf.mu.Unlock()

		if isLeader {
			rf.mu.Lock()
			rf.resetTimer()
			rf.mu.Unlock()
		} else {
			if time.Now().After(lastHeartbeat.Add(time.Duration(timeout) * time.Millisecond)) {
				rf.tryElection()
			}
		}
		// calculate the time to wake up that is approch to the
		time.Sleep(10 * time.Millisecond)
	}
}

// This function does not claim a lock
func (rf *Raft) commiter() {

	for rf.killed() == false {

		message := make([]ApplyMsg, 0)
		rf.mu.Lock()
		for rf.status.lastApplied < rf.status.commitIndex {
			rf.status.lastApplied += 1
			// do not commit the entry at index 0 which is a placeholder
			if rf.status.lastApplied == 0 {
				continue
			}
			entry := rf.status.logs[rf.status.lastApplied]
			DPrintf("server: %v, sending entry: [pos: %v, term: %v] to state machine", rf.me, entry.Pos, entry.TermReceive)
			message = append(message, ApplyMsg{CommandValid: true, Command: entry.Command, CommandIndex: rf.status.lastApplied})
		}
		rf.mu.Unlock()

		for _, m := range message {
			rf.applyCh <- m
		}

		time.Sleep(10 * time.Millisecond)
	}

}

// This function does not claim lock itself
func (rf *Raft) prepareVoteArgs() RequestVoteArgs {
	lastLogTerm := 0
	if len(rf.status.logs) > 0 {
		lastLogTerm = rf.status.logs[len(rf.status.logs)-1].TermReceive
	}
	args := RequestVoteArgs{
		Term:         rf.status.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.status.logs) - 1,
		LastLogTerm:  lastLogTerm,
	}
	return args
}

func (rf *Raft) tryElection() {
	rf.mu.Lock()
	rf.resetTimer()
	me := rf.me
	rf.state = Candidate
	rf.status.votedFor = me
	rf.status.currentTerm = rf.status.currentTerm + 1
	rf.status.leaderId = -1
	term := rf.status.currentTerm
	DPrintf("server %v[%v] starts a new election\n", rf.me, term)
	prepArgs := rf.prepareVoteArgs()
	rf.mu.Unlock()

	done := false
	count := 1
	// Need this to eliminate the situation that another
	// election gives higher term and cause 2 leaders granted

	for i := range rf.peers {
		if i == me {
			continue
		}
		// send requestVote to other peers
		go func(server int) {

			args := prepArgs

			reply := RequestVoteReply{}

			if !rf.sendRequestVote(server, &args, &reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.status.currentTerm != term {
				return
			}

			if reply.Term > rf.status.currentTerm {
				rf.status.currentTerm = reply.Term
				rf.state = Follower
				rf.status.votedFor = -1
				DPrintf("candidate %v return to a follower\n", rf.me)
				return
			}

			if reply.VoteGranted {

				count += 1
				if done || count <= len(rf.peers)/2 || rf.state != Candidate {
					return
				}

				done = true
				rf.status.leaderId = rf.me
				rf.state = Leader

				temp := make([][]int, 0)
				for _, entry := range rf.status.logs {
					a := []int{entry.Pos, entry.TermReceive}
					temp = append(temp, a)
				}
				DPrintf("New leader: %v[%v] is selected to be a leader. its log is %v \n", rf.me, rf.status.currentTerm, temp)

				rf.initializeLogIndexes(len(rf.status.logs))
				go rf.heartbeat()
			}
		}(i)
	}
}

// This function does not claim lock itself
func (rf *Raft) initializeLogIndexes(index int) {
	for i := range rf.status.leaderState.nextIndex {
		rf.status.leaderState.nextIndex[i] = index
	}

	for i := range rf.status.leaderState.matchIndex {
		rf.status.leaderState.matchIndex[i] = 0
	}
}

// This function does not claim lock itself
func (rf *Raft) updateCommitIndex() {
	a := make([]int, len(rf.peers))
	copy(a, rf.status.leaderState.matchIndex)
	sort.Ints(a)

	mid := len(rf.peers) / 2
	if len(rf.peers)%2 == 0 {
		mid -= 1
	}

	for i := mid; i >= 0; i-- {
		if rf.status.commitIndex < a[i] && rf.status.logs[a[i]].TermReceive == rf.status.currentTerm {
			rf.status.commitIndex = a[i]
			DPrintf("Leader %v update commitIndex to be: %v", rf.me, a[i])
			break
		}
	}
}

// This function does not claim lock itself
func (rf *Raft) prepareAppendArgs(peerId int, term int, leaderCommit int) (AppendEntriesArgs, AppendEntriesReply, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	nextIndex := rf.status.leaderState.nextIndex[peerId]
	entries := make([]Log, 0)
	for i := nextIndex; i < len(rf.status.logs); i++ {
		entries = append(entries, rf.status.logs[i])
	}

	prevLogIndex := nextIndex - 1
	prevLogTerm := -1
	if prevLogIndex >= 0 {
		prevLogTerm = rf.status.logs[prevLogIndex].TermReceive
	}

	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		Entries:      entries,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: leaderCommit,
	}

	reply := AppendEntriesReply{}

	return args, reply, len(rf.status.logs) - 1

}

func (rf *Raft) heartbeat() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		term := rf.status.currentTerm
		me := rf.me
		leaderCommit := rf.status.commitIndex

		rf.mu.Unlock()
		for i, peer := range rf.peers {

			if i == me {
				continue
			}
			// Send heartbeats to all followers
			go func(p *labrpc.ClientEnd, peerId int) {

				args, reply, lastEntryIndex := rf.prepareAppendArgs(peerId, term, leaderCommit)

				ok := p.Call("Raft.AppendEntries", &args, &reply)

				if !ok {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.status.currentTerm != term {
					return
				}

				if reply.Success {
					rf.status.leaderState.nextIndex[peerId] = max(lastEntryIndex+1, rf.status.leaderState.nextIndex[peerId])
					rf.status.leaderState.matchIndex[peerId] = max(lastEntryIndex, rf.status.leaderState.matchIndex[peerId])
					// check commitIndex after a successfuly append
					rf.updateCommitIndex()

				} else {
					// AppendEntries fails because of outdated term
					if rf.status.currentTerm < reply.Term || reply.Term == 0 {
						if reply.Term != 0 {
							rf.status.currentTerm = reply.Term
						}
						rf.state = Follower
						rf.status.votedFor = -1

					} else {
						// AppendEntries fails becuase of log inconsistency
						if reply.BackupIndex != -1 {
							rf.status.leaderState.nextIndex[peerId] = reply.BackupIndex
						} else {
							DPrintf("error unexpected! with backup index %v and term %v\n", reply.BackupIndex, reply.Term)
						}
					}
				}

			}(peer, i)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}

	rf.mu = sync.Mutex{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.status = State{}
	rf.status.votedFor = -1
	rf.status.leaderId = -1
	rf.status.lastApplied = -1
	rf.status.commitIndex = 0
	rf.status.logs = make([]Log, 1)
	rf.status.leaderState = LeaderState{}
	rf.status.leaderState.nextIndex = make([]int, len(peers))
	rf.status.leaderState.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh
	rf.state = Follower
	rf.resetTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start an commiter that periodically send committed logs to state machine
	go rf.commiter()

	return rf
}
