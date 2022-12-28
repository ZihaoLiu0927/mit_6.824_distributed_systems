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

	"bytes"
	"log"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"

	"math/rand"
	"sort"
	"time"

	"6.824/labgob"
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
	RawIndex    int
}

type LeaderState struct {
	nextIndex  []int
	matchIndex []int
}

type Pstate struct {
	CurrentTerm       int
	VotedFor          int
	Logs              []Log
	LatestIndex       int // keep track of the last index in log array, the index is independent of the snapshot trims
	LastIncludedIndex int
	LastIncludedTerm  int
}

type Vstate struct {
	commitIndex int
	lastApplied int
	leaderId    int
	leaderState LeaderState
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
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

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term    int
	Success bool
}

type Status string

const (
	Leader    Status = "leader"
	Candidate Status = "candidate"
	Follower  Status = "follower"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu            sync.Mutex          // Lock to protect shared access to this peer's state
	peers         []*labrpc.ClientEnd // RPC end points of all peers
	persister     *Persister          // Object to hold this peer's persisted state
	me            int                 // this peer's index into peers[]
	dead          int32               // set by Kill()
	pstate        Pstate
	vstate        Vstate
	lastHeartbeat time.Time
	timeout       int
	status        Status
	applyCh       chan ApplyMsg
	LastSnapshot  []byte
}

// Helper functions
func (this Log) equal(another Log) bool {
	if this.TermReceive == another.TermReceive && this.RawIndex == another.RawIndex {
		return true
	}
	return false
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

// Used for debugging
func (rf *Raft) printLog() [][]int {
	temp := make([][]int, 0)
	for _, entry := range rf.pstate.Logs {
		a := []int{entry.RawIndex, entry.TermReceive}
		temp = append(temp, a)
	}
	return temp
}

// Leader steps back to be a follower
// Must claim a lock before calling this
func (rf *Raft) stepback(term int) {
	rf.status = Follower
	rf.pstate.CurrentTerm = term
	rf.pstate.VotedFor = -1
	rf.vstate.leaderId = -1
}

// Follower steps forward to be a candidate
// Must claim a lock before calling this
func (rf *Raft) stepforward() int {
	rf.status = Candidate
	rf.pstate.VotedFor = rf.me
	rf.pstate.CurrentTerm = rf.pstate.CurrentTerm + 1
	rf.vstate.leaderId = -1
	return rf.pstate.CurrentTerm
}

// Access the latest log index. The persist func does not claim a lock itself.
func (rf *Raft) rawToLocalIndex(rawIndex int) int {
	return rawIndex - (rf.pstate.LastIncludedIndex + 1)
}

// Access the latest log index. The persist func does not claim a lock itself.
func (rf *Raft) localToRawIndex(localIndex int) int {
	return localIndex + (rf.pstate.LastIncludedIndex + 1)
}

// The persist func does not claim a lock itself.
func (rf *Raft) getLastLogEntry() (Log, int) {
	return rf.getLogEntry(rf.pstate.LatestIndex)
}

// The persist func does not claim a lock itself.
func (rf *Raft) getLogEntry(rawIndex int) (Log, int) {
	idx := rf.rawToLocalIndex(rawIndex)
	if idx < 0 {
		return Log{"Snapshot", rf.pstate.LastIncludedTerm, rf.pstate.LastIncludedIndex}, idx
	}
	return rf.pstate.Logs[idx], idx
}

// The persist func does not claim a lock itself.
func (rf *Raft) updateSnapshotInfo(index int, term int, data []byte) {
	rf.pstate.LastIncludedIndex = index
	rf.pstate.LastIncludedTerm = term
	rf.LastSnapshot = data
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.pstate.CurrentTerm, rf.status == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.

// The persist func does not claim a lock itself.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.pstate)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	buf := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(buf)
	pstate := Pstate{}

	if dec.Decode(&pstate) != nil {
		log.Fatal("Error in decoding the persistent states!\n")
		return
	}
	rf.pstate = pstate
	//rf.LastSnapshot = rf.persister.ReadSnapshot()
	rf.vstate.lastApplied = rf.pstate.LastIncludedIndex
}

func (rf *Raft) persistStateAndSnapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.pstate)
	stateData := w.Bytes()

	if rf.LastSnapshot != nil {
		rf.persister.SaveStateAndSnapshot(stateData, rf.LastSnapshot)
	} else {
		rf.persister.SaveStateAndSnapshot(stateData, nil)
	}
}

func (rf *Raft) trimLogAndUpdateSnapInfo(localIdx int, LastIncludedIndex int, LastIncludedTerm int, snapshot []byte) {
	if localIdx == -1 {
		// discard the entire log if input index is -1
		rf.pstate.Logs = make([]Log, 0)
		rf.pstate.LatestIndex = LastIncludedIndex
	} else {
		// discard the logs before and at local index
		rf.pstate.Logs = rf.pstate.Logs[localIdx+1:]
	}
	rf.updateSnapshotInfo(LastIncludedIndex, LastIncludedTerm, snapshot)
}

// InstallSnapshot RPC handler.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persistStateAndSnapshot()

	reply.Term = rf.pstate.CurrentTerm
	reply.Success = false

	// install snapshot rejected because the leader is outdated
	if rf.pstate.CurrentTerm > args.Term {
		return
	}

	// install snapshot rejected because the incoming snapshot has been installed already
	realIdx := rf.rawToLocalIndex(args.LastIncludedIndex)
	if realIdx < 0 {
		return
	}

	rf.applyCh <- ApplyMsg{SnapshotValid: true, Snapshot: args.Data,
		SnapshotTerm: args.LastIncludedTerm, SnapshotIndex: args.LastIncludedIndex}

	reply.Success = true
	rf.vstate.lastApplied = max(rf.vstate.lastApplied, args.LastIncludedIndex)

	if args.LastIncludedIndex > rf.pstate.LatestIndex {
		rf.trimLogAndUpdateSnapInfo(-1, args.LastIncludedIndex, args.LastIncludedTerm, args.Data)
		DPrintf("server %v GETs and executes an INSTALL SNAPSHOT request with newer snapshot, args' lastIncludedIndex: %v, curr lastIncludedIndex:%v, curr log length: %v", rf.me, args.LastIncludedIndex, rf.pstate.LastIncludedIndex, len(rf.pstate.Logs))
		return
	}

	compareLog, localIdx := rf.getLogEntry(args.LastIncludedIndex)

	if compareLog.RawIndex == args.LastIncludedIndex &&
		compareLog.TermReceive == args.LastIncludedTerm {
		rf.trimLogAndUpdateSnapInfo(localIdx, args.LastIncludedIndex, args.LastIncludedTerm, args.Data)
		DPrintf("server %v GETs and executes an INSTALL SNAPSHOT request with snapshot index in its log, args' lastIncludedIndex: %v, curr lastIncludedIndex:%v, curr log length: %v", rf.me, args.LastIncludedIndex, rf.pstate.LastIncludedIndex, len(rf.pstate.Logs))
	} else {
		rf.trimLogAndUpdateSnapInfo(-1, args.LastIncludedIndex, args.LastIncludedTerm, args.Data)
		DPrintf("server %v GETs and executes an INSTALL SNAPSHOT request, discard the entire log, args' lastIncludedIndex: %v, curr lastIncludedIndex:%v, curr log length: %v", rf.me, args.LastIncludedIndex, rf.pstate.LastIncludedIndex, len(rf.pstate.Logs))

	}
}

// Deprecated: A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persistStateAndSnapshot()

	// discard logs before and at index
	log, localIdx := rf.getLogEntry(index)
	rf.trimLogAndUpdateSnapInfo(localIdx, log.RawIndex, log.TermReceive, snapshot)
	rf.vstate.lastApplied = max(rf.vstate.lastApplied, log.RawIndex)

	DPrintf("server %v SNAPSHOTS on its own log at real index %v. Current log length: %v, lastIncludedIndex: %v", rf.me, index, len(rf.pstate.Logs), rf.pstate.LastIncludedIndex)
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.pstate.CurrentTerm
	reply.VoteGranted = false

	if args.Term < rf.pstate.CurrentTerm || rf.killed() {
		return
	}

	if args.Term > rf.pstate.CurrentTerm {
		rf.stepback(args.Term)
	}

	// Election restriction in paper 5.4.1:
	// the voter denies its vote if its own log is more up-to-date than that of the candidate
	if rf.pstate.VotedFor == -1 || rf.pstate.VotedFor == args.CandidateId {

		// Calculate the last log's real index in the this machine
		lastLog, _ := rf.getLastLogEntry()
		myLastLogTerm := lastLog.TermReceive

		// compare their terms first
		if args.LastLogTerm > myLastLogTerm {
			reply.VoteGranted = true
			rf.pstate.VotedFor = args.CandidateId
			rf.resetTimer()

			DPrintf("server %v with last log[%v, %v] grants candidate %v last log[%v, %v] bc newer term\n",
				rf.me, lastLog.RawIndex, lastLog.TermReceive,
				args.CandidateId, args.LastLogIndex, args.LastLogTerm)
			return
		}

		// if tied on the term, then compare log length
		if args.LastLogTerm == myLastLogTerm && rf.pstate.LatestIndex <= args.LastLogIndex {
			reply.VoteGranted = true
			rf.pstate.VotedFor = args.CandidateId
			rf.resetTimer()

			DPrintf("server %v with last log[%v, %v] grants candidate %v last log[%v, %v] bc equal or longer log length\n",
				rf.me, lastLog.RawIndex, lastLog.TermReceive,
				args.CandidateId, args.LastLogIndex, args.LastLogTerm)

		}

	}
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.pstate.CurrentTerm
	reply.Success = false
	reply.BackupIndex = -1

	// step 1:  Reply FALSE if term < CurrentTerm
	if args.Term < rf.pstate.CurrentTerm || rf.killed() {
		return
	}

	if args.Term > rf.pstate.CurrentTerm {
		rf.stepback(args.Term)
	}

	rf.vstate.leaderId = args.LeaderId

	// Access the last log in this machine
	lastLog, _ := rf.getLastLogEntry()
	rf.resetTimer()

	// step 2: Reply FALSE if log doesn’t contain an entry at prevLogIndex
	if args.PrevLogIndex > 0 && args.PrevLogIndex > rf.pstate.LatestIndex {
		reply.BackupIndex = rf.pstate.LatestIndex + 1
		DPrintf("Heartbeat for %v[%v], from leader %v[%v], log append rejected bc no entry at prevLogIndex %v, update backup index = %v",
			rf.me, rf.pstate.CurrentTerm, args.LeaderId, args.Term, args.PrevLogIndex, reply.BackupIndex)
		return
	}

	// Get the prevLogIndex matched entry in this machine
	prevLog, prevLogLocalIndex := rf.getLogEntry(args.PrevLogIndex)

	//step 3: Reply FALSE if log does contain an entry at prevLogIndex but term does not matche prevLogTerm
	if args.PrevLogIndex >= 0 && prevLog.TermReceive != args.PrevLogTerm {
		i := prevLogLocalIndex
		badTerm := prevLog.TermReceive
		for ; i >= 0 && rf.pstate.Logs[i].TermReceive == badTerm; i-- {
		}
		// restore the raw index for back up
		reply.BackupIndex = rf.localToRawIndex(i + 1)
		DPrintf("Heartbeat for %v[%v], from leader %v[%v], log append rejected bc entry mismatch at %v, update backup index = %v",
			rf.me, rf.pstate.CurrentTerm, args.LeaderId, args.Term, args.PrevLogIndex, reply.BackupIndex)
		return
	}

	// save the last log info on this machine before updating the log entry array
	olgLatestIndex := lastLog.RawIndex

	// step 4 and 5: append all new entries and reply TRUE
	curr := prevLogLocalIndex + 1
	count := 0
	for _, entry := range args.Entries {
		if curr == len(rf.pstate.Logs) {
			break
		}
		// trim the log if unmatching found
		if !rf.pstate.Logs[curr].equal(entry) {
			rf.pstate.Logs = rf.pstate.Logs[0:curr]
			// need to update the last raw index after trimming the log
			olgLatestIndex = -1
			break
		}
		curr += 1
		count += 1
	}
	if count < len(args.Entries) {
		rf.pstate.Logs = append(rf.pstate.Logs, args.Entries[count:]...)
	}

	// if the follower has logs after the entries sent by leader, we want to keep them as long as the sent entries matched.
	if len(args.Entries) > 0 {
		rf.pstate.LatestIndex = max(args.Entries[len(args.Entries)-1].RawIndex, olgLatestIndex)
	}

	// step 6: leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.vstate.commitIndex && len(args.Entries) > 0 {
		rf.vstate.commitIndex = min(args.LeaderCommit, args.Entries[len(args.Entries)-1].RawIndex)

	} else if args.LeaderCommit > rf.vstate.commitIndex && len(args.Entries) == 0 {
		rf.vstate.commitIndex = min(args.LeaderCommit, args.PrevLogIndex)
	}

	reply.Success = true

	currlog, _ := rf.getLastLogEntry()
	DPrintf("Heartbeat for %v[%v], from leader %v[%v], follower's previous logs is: [pos: %v, term: %v], current logs is [pos: %v, term: %v]",
		rf.me, rf.pstate.CurrentTerm, args.LeaderId, args.Term, lastLog.RawIndex, lastLog.TermReceive, currlog.RawIndex, currlog.TermReceive)
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	peer := rf.peers[server]
	return peer.Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	peer := rf.peers[server]
	return peer.Call("Raft.InstallSnapshot", args, reply)
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
	defer rf.persist()

	term, isLeader = rf.pstate.CurrentTerm, rf.status == Leader

	if !isLeader || rf.killed() {
		return index, term, isLeader
	}

	index = rf.vstate.leaderState.nextIndex[rf.me]
	rf.pstate.Logs = append(rf.pstate.Logs,
		Log{
			Command:     command,
			TermReceive: rf.pstate.CurrentTerm,
			RawIndex:    index,
		})
	rf.pstate.LatestIndex = index
	rf.vstate.leaderState.nextIndex[rf.me] += 1
	rf.vstate.leaderState.matchIndex[rf.me] = index

	DPrintf("New command come to leader %v[%v], command position: %v", rf.me, rf.pstate.CurrentTerm, index)
	lastLog, _ := rf.getLastLogEntry()

	DPrintf("Leader %v[%v] current logs is: [pos: %v, term: %v]; current commitIndex is: %v\n", rf.me,
		rf.pstate.CurrentTerm, lastLog.RawIndex, lastLog.TermReceive, rf.vstate.commitIndex)

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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) generateTimeOut() int {
	return 200 + rand.Intn(250)
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
		isLeader := rf.status == Leader
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
		for rf.vstate.lastApplied < rf.vstate.commitIndex {
			rf.vstate.lastApplied += 1
			// do not commit the entry at index 0 which is a placeholder
			if rf.vstate.lastApplied == 0 {
				continue
			}
			// entry is guaranteed not being snapshoted yet
			entry, localIdx := rf.getLogEntry(rf.vstate.lastApplied)
			DPrintf("server %v applied a log to state machine at local index %v, command is %v", rf.me, localIdx, entry.Command)
			//DPrintf("server: %v, sending entry: [pos: %v, term: %v] to state machine", rf.me, entry.Pos, entry.TermReceive)
			message = append(message, ApplyMsg{CommandValid: true, Command: entry.Command, CommandIndex: rf.vstate.lastApplied})
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

	lastlog, _ := rf.getLastLogEntry()
	lastLogTerm = lastlog.TermReceive

	args := RequestVoteArgs{
		Term:         rf.pstate.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.pstate.LatestIndex,
		LastLogTerm:  lastLogTerm,
	}
	return args
}

func (rf *Raft) tryElection() {
	rf.mu.Lock()
	rf.resetTimer()
	me := rf.me
	term := rf.stepforward()
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
			defer rf.persist()

			if rf.pstate.CurrentTerm != term {
				return
			}

			if reply.Term > rf.pstate.CurrentTerm {
				rf.stepback(args.Term)
				DPrintf("candidate %v return to a follower\n", rf.me)
				return
			}

			if reply.VoteGranted {

				count += 1
				if done || count <= len(rf.peers)/2 || rf.status != Candidate {
					return
				}

				done = true
				// become a leader
				rf.vstate.leaderId = rf.me
				rf.status = Leader

				DPrintf("New leader: %v[%v] is selected to be a leader. its log is %v \n", rf.me, rf.pstate.CurrentTerm, rf.printLog())

				rf.initializeLogIndexes(rf.pstate.LatestIndex + 1)
				go rf.heartbeat()
			}
		}(i)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.persist()
}

// This function does not claim lock itself
func (rf *Raft) initializeLogIndexes(index int) {
	for i := range rf.vstate.leaderState.nextIndex {
		rf.vstate.leaderState.nextIndex[i] = index
	}

	for i := range rf.vstate.leaderState.matchIndex {
		rf.vstate.leaderState.matchIndex[i] = 0
	}
}

// This function does not claim lock itself
func (rf *Raft) updateCommitIndex() {
	a := make([]int, len(rf.peers))
	copy(a, rf.vstate.leaderState.matchIndex)
	sort.Ints(a)

	mid := len(rf.peers) / 2
	if len(rf.peers)%2 == 0 {
		mid -= 1
	}

	for i := mid; i >= 0; i-- {
		// entry is guaranteed not being snapshoted because this index is not committed yet
		// therefore, entry must exist.
		entry, _ := rf.getLogEntry(a[i])
		if rf.vstate.commitIndex < a[i] && entry.TermReceive == rf.pstate.CurrentTerm {
			rf.vstate.commitIndex = a[i]
			DPrintf("Leader %v update commitIndex to be: %v", rf.me, a[i])
			break
		}
	}
}

// This function does not claim lock itself
func (rf *Raft) prepareAppendArgs(term int, leaderCommit int, nextIndex int, nextRealIndex int) (AppendEntriesArgs, AppendEntriesReply, int) {
	if nextRealIndex < 0 {
		return AppendEntriesArgs{}, AppendEntriesReply{}, -1
	}

	entries := make([]Log, 0)
	for i := nextRealIndex; i < len(rf.pstate.Logs); i++ {
		entries = append(entries, rf.pstate.Logs[i])
	}

	prevLogIndex := nextIndex - 1
	prevLogTerm := -1
	if prevLogIndex >= 0 {
		log, _ := rf.getLogEntry(prevLogIndex)
		prevLogTerm = log.TermReceive
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

	lastCompare := rf.pstate.LatestIndex

	return args, reply, lastCompare

}

func (rf *Raft) processAppendEntries(peerId int, term int, lastCompare int, args AppendEntriesArgs, reply AppendEntriesReply) {
	if !rf.sendAppendEntries(peerId, &args, &reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.pstate.CurrentTerm != term {
		return
	}

	if reply.Success {
		// max to avoid old update caused by internet delay
		rf.vstate.leaderState.nextIndex[peerId] = max(lastCompare+1, rf.vstate.leaderState.nextIndex[peerId])
		rf.vstate.leaderState.matchIndex[peerId] = max(lastCompare, rf.vstate.leaderState.matchIndex[peerId])
		// check commitIndex after a successfuly append
		rf.updateCommitIndex()

	} else {
		// AppendEntries fails because of outdated term
		if rf.pstate.CurrentTerm < reply.Term {
			rf.stepback(args.Term)

		} else {
			// AppendEntries fails becuase of log inconsistency
			if reply.BackupIndex != -1 {
				// Use min() to avoid old(reverse) update caused by internet delay
				rf.vstate.leaderState.nextIndex[peerId] = min(reply.BackupIndex, rf.vstate.leaderState.nextIndex[peerId])
			} else {
				DPrintf("error unexpected! with backup index %v and term %v\n", reply.BackupIndex, reply.Term)
			}
		}
	}
}
func (rf *Raft) prepareSnapshotArgs(term int, leaderCommit int, nextIndex int, nextRealIndex int) (InstallSnapshotArgs, InstallSnapshotReply, int) {
	args := InstallSnapshotArgs{
		Term:              term,
		LeaderId:          rf.vstate.leaderId,
		LastIncludedIndex: rf.pstate.LastIncludedIndex,
		LastIncludedTerm:  rf.pstate.LastIncludedTerm,
		Data:              rf.LastSnapshot,
	}
	reply := InstallSnapshotReply{}
	SnapshotIndex := rf.pstate.LastIncludedIndex
	return args, reply, SnapshotIndex
}

func (rf *Raft) processInstallSnapshot(peerId int, term int, SnapshotIndex int, args InstallSnapshotArgs, reply InstallSnapshotReply) {
	if !rf.sendInstallSnapshot(peerId, &args, &reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.pstate.CurrentTerm != term {
		return
	}

	if reply.Success {
		// max to avoid old update caused by internet delay
		rf.vstate.leaderState.nextIndex[peerId] = max(SnapshotIndex+1, rf.vstate.leaderState.nextIndex[peerId])
		rf.vstate.leaderState.matchIndex[peerId] = max(SnapshotIndex, rf.vstate.leaderState.matchIndex[peerId])
		// check commitIndex after a successfuly install
		rf.updateCommitIndex()
	} else {
		// Install snapshot fails because of outdated term
		if rf.pstate.CurrentTerm < reply.Term {
			rf.stepback(args.Term)
		}
	}
}

func (rf *Raft) heartbeat() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.status != Leader {
			rf.mu.Unlock()
			return
		}
		term := rf.pstate.CurrentTerm
		me := rf.me
		leaderCommit := rf.vstate.commitIndex
		rf.mu.Unlock()

		for i := range rf.peers {

			if i == me {
				continue
			}
			// Send heartbeats to all followers
			go func(peerId int) {

				rf.mu.Lock()
				nextIndex := rf.vstate.leaderState.nextIndex[peerId]
				nextRealIndex := rf.rawToLocalIndex(nextIndex)

				// if the nextIndex for the follower is being after the snapshot of current leader's logs, send appendEntries RPC.
				// else if the nextIndex for the follower is lagging before the snapshot point of the current leader's logs, send installSnapshpt RPC instead.
				if nextRealIndex >= 0 {
					args, reply, lastCompare := rf.prepareAppendArgs(term, leaderCommit, nextIndex, nextRealIndex)
					rf.mu.Unlock()
					rf.processAppendEntries(peerId, term, lastCompare, args, reply)
				} else {
					args, reply, lastCompare := rf.prepareSnapshotArgs(term, leaderCommit, nextIndex, nextRealIndex)
					rf.mu.Unlock()
					rf.processInstallSnapshot(peerId, term, lastCompare, args, reply)
				}
			}(i)
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
	rf.pstate = Pstate{}
	rf.pstate.VotedFor = -1
	rf.pstate.Logs = make([]Log, 1)
	rf.pstate.LastIncludedIndex = -1
	rf.pstate.LatestIndex = 0

	rf.vstate = Vstate{}
	rf.vstate.leaderId = -1
	rf.vstate.lastApplied = 0
	rf.vstate.commitIndex = 0
	rf.vstate.leaderState = LeaderState{}
	rf.vstate.leaderState.nextIndex = make([]int, len(peers))
	rf.vstate.leaderState.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh
	rf.status = Follower
	rf.resetTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start an commiter that periodically send committed logs to state machine
	go rf.commiter()

	return rf
}
