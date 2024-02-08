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
	"bytes"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
	"io/ioutil"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// TODO: Faster algorithm of leader backing up incorrect logs

const debug = false
const rpcTimeout = 700 * time.Millisecond

type Command interface{}

type rfState string

const (
	rfStateFollower  rfState = "f"
	rfStateCandidate         = "c"
	rfStateLeader            = "l"
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
	Command      Command
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command Command
	Term    int
}

type Log []LogEntry

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	logger          *log.Logger
	applyCh         chan ApplyMsg // Channel for applying commands
	dead            chan struct{}
	electionTicker  *RandomTicker
	heartbeatTicker *time.Ticker

	// Your data here (2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// Persistent state
	currentTerm int // Latest term seen (0 on first boot)
	votedFor    int // Peer that recieved vote in current term (-1 if none)
	log         Log

	// Volatile state for all
	state       rfState // Current state of the server (follower, candidate, leader)
	commitIndex int     // Index of highest log entry known to be commited (0 initially)
	lastApplied int     // Index of highest log entry applied (0 initially)

	// Volatile state for leaders
	nextIndex  []int // For each peer, index of the next log entry to send (leader last log index + 1 initially)
	matchIndex []int // For each peer, index of the highest log entry known to be replicated (0 initially)
}

type PersistentState struct {
	CurrentTerm int
	VotedFor int
	Log Log
}

func (rf *Raft) setState(state rfState) {
	rf.state = state
	rf.logger.SetPrefix(fmt.Sprintf("rf[%d %v] ", rf.me, rf.state))
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == rfStateLeader
}

func (log Log) at(oneBasedIdx int) (entry LogEntry) {
	if oneBasedIdx <= 0 || oneBasedIdx > len(log) {
		return
	}
	entry = log[oneBasedIdx-1]
	return
}

func (log Log) last() (entry LogEntry) {
	if len(log) == 0 {
		return
	}
	entry = log[len(log)-1]
	return
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	state := PersistentState {
		CurrentTerm: rf.currentTerm,
		VotedFor: rf.votedFor,
		Log: rf.log,
	}
	e.Encode(state)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state
		rf.votedFor = -1
		rf.currentTerm = 0
		rf.log = Log{}
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	state := PersistentState{}
	if err := d.Decode(&state); err != nil {
		panic(fmt.Sprintf("Couldn't decode persistent state: %v", err))
	} else {
	  rf.votedFor = state.VotedFor
	  rf.currentTerm = state.CurrentTerm
	  rf.log = state.Log
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int // Candidate's term
	CandidateId  int // Candidate that is requesting a vote
	LastLogIndex int // Index of candidate's last log entry
	LastLogTerm  int // Term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int  // Term, for candidate to update intself to
	VoteGranted bool // Candidate recieved a vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	rf.persist()
	defer rf.mu.Unlock()
	defer func(reply *RequestVoteReply) {
		if reply.VoteGranted {
			rf.electionTicker.Reset()
			rf.votedFor = args.CandidateId
		}
	}(reply)

	lastEntry := rf.log.last()
	rf.logger.Printf("Got RequestVote %+v", args)
	rf.logger.Printf("currentTerm=%v votedFor=%v lastEntryTerm=%v", rf.currentTerm, rf.votedFor, lastEntry.Term)
	defer rf.logger.Printf("Replied to RequestVote %+v", reply)

	canVote := rf.currentTerm != args.Term || rf.votedFor == -1 || rf.votedFor == args.CandidateId

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.setState(rfStateFollower)
	}

	reply.Term = rf.currentTerm

	staleTerm := args.Term < rf.currentTerm
	if staleTerm || !canVote {
		reply.VoteGranted = false
		return
	}

	upToDate := args.LastLogTerm > lastEntry.Term || (args.LastLogTerm == lastEntry.Term && args.LastLogIndex >= len(rf.log))
	if !upToDate {
		reply.VoteGranted = false
		return
	}
	reply.VoteGranted = true
	rf.persist()
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	rf.persist()
	defer rf.mu.Unlock()

	if len(args.Entries) > 0 {
		rf.logger.Printf("Got AppendEntries %+v", args)
		rf.logger.Printf("currentTerm=%v commitIndex=%v len(log)=%v", rf.currentTerm, rf.commitIndex, len(rf.log))
		defer rf.logger.Printf("Replied to AppendEntries %+v", reply)
	}

	reply.Term = rf.currentTerm
	reply.Success = true

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		rf.setState(rfStateFollower)
		rf.currentTerm = args.Term
	}
	rf.electionTicker.Reset()

	prevEntry := rf.log.at(args.PrevLogIndex)
	if args.PrevLogIndex != 0 && prevEntry.Term == 0 {
		reply.Success = false
		return
	}

	if prevEntry.Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	var i int
	for i = 0; i < len(args.Entries); i++ {
		idx := args.PrevLogIndex + i
		entry := rf.log.at(idx + 1)
		if entry.Term == 0 {
			break
		}
		if entry.Term != args.Entries[i].Term {
			rf.log = rf.log[:idx]
			break
		}
	}

	newEntries := args.Entries[i:]
	if len(newEntries) != 0 {
		rf.logger.Printf("Appended new entries %v", newEntries)
		rf.log = append(rf.log, newEntries...)
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = minimum(args.LeaderCommit, len(rf.log))
		rf.logger.Printf("Committed up to %v", rf.commitIndex)
	}

	rf.persist()
	go rf.applyCommitted()
}

func (rf *Raft) getAppendEntriesArgs(server int, heartbeat bool) AppendEntriesArgs {
	var prevIndex int
	if heartbeat {
		prevIndex = len(rf.log)
	} else {
		prevIndex = maximum(rf.nextIndex[server]-1, 0)
	}
	prevTerm := rf.log.at(prevIndex).Term
	entries := rf.log[prevIndex:]
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	return args
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, timeoutD time.Duration) bool {
	rf.logger.Printf("Send RequestVote to rf[%d]: %+v", server, args)

	okC := make(chan bool)
	timeout := time.NewTimer(timeoutD)
	go func() {
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		okC <- ok
	}()
	select {
	case ok := <-okC:
		return ok
	case <-timeout.C:
		return false
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, timeoutD time.Duration) bool {
	if len(args.Entries) != 0 {
		rf.logger.Printf("Send AppendEntries to rf[%d]: %+v", server, args)
	}

	okC := make(chan bool)
	timeout := time.NewTimer(timeoutD)
	go func() {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		okC <- ok
	}()
	select {
	case ok := <-okC:
		return ok
	case <-timeout.C:
		return false
	}
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
func (rf *Raft) Start(command Command) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logger.Printf("Start(%v)", command)

	isLeader = rf.state == rfStateLeader
	if !isLeader {
		return
	}
	rf.log = append(rf.log, LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	})
	index = len(rf.log)
	term = rf.currentTerm
	go rf.replicateEntries()
	return
}

func (rf *Raft) applyCommitted() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		rf.logger.Printf("Applying command idx=%d", rf.lastApplied)
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log.at(rf.lastApplied).Command,
			CommandIndex: rf.lastApplied,
		}
		rf.applyCh <- msg
	}
}

func (rf *Raft) replicateEntries() {
	mu := sync.Mutex{}
	cond := sync.NewCond(&mu)
	replicated := 1
	finished := 0

	rf.mu.Lock()
	logLen := len(rf.log)
	rf.mu.Unlock()

	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			success := rf.replicateOnServer(server)
			mu.Lock()
			if success {
				replicated++
			}
			finished++
			cond.Broadcast()
			mu.Unlock()
		}(server)
	}

	mu.Lock()
	for replicated < len(rf.peers)/2+1 && finished < len(rf.peers)-1 {
		cond.Wait()
	}
	mu.Unlock()

	rf.mu.Lock()
	if replicated >= len(rf.peers)/2+1 {
		rf.commitIndex = maximum(rf.commitIndex, logLen)
		rf.logger.Printf("Replicated and committed up to %d", rf.commitIndex)
	}
	rf.persist()
	rf.mu.Unlock()
	go rf.applyCommitted()
	return
}

func (rf *Raft) replicateOnServer(server int) (replicated bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for len(rf.log) >= rf.nextIndex[server] {
		if rf.state != rfStateLeader {
			replicated = false
			return
		}
		args := rf.getAppendEntriesArgs(server, false)
		rf.mu.Unlock()
		var reply AppendEntriesReply
		ok := rf.sendAppendEntries(server, &args, &reply, rpcTimeout)
		rf.mu.Lock()
		if !ok {
			continue
		}
		if reply.Term > rf.currentTerm {
			rf.setState(rfStateFollower)
			rf.currentTerm = reply.Term
			replicated = false
			return
		}
		if reply.Success {
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
		} else {
			rf.nextIndex[server]--
		}
	}
	replicated = true
	return
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
	// Your code here, if desired.
	rf.heartbeatTicker.Stop()
	rf.electionTicker.Stop()
	rf.dead <- struct{}{}
}

func (rf *Raft) ticker() {
	for {
		select {
		case <-rf.dead:
			return
		case <-rf.heartbeatTicker.C:
			rf.mu.Lock()
			if rf.state == rfStateLeader {
				rf.mu.Unlock()
				go rf.sendHeartbeats()
			} else {
				rf.mu.Unlock()
			}
		case <-rf.electionTicker.C:
			rf.mu.Lock()
			if rf.state != rfStateLeader {
				rf.mu.Unlock()
				go rf.startElection()
			} else {
				rf.mu.Unlock()
			}
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.setState(rfStateCandidate)
	rf.currentTerm += 1
	rf.votedFor = rf.me
	startingTerm := rf.currentTerm
	rf.mu.Unlock()

	elected := rf.gatherVotes()

	rf.mu.Lock()
	rf.persist()
	stillCandidate := rf.state == rfStateCandidate
	sameTerm := startingTerm == rf.currentTerm
	if stillCandidate && elected && sameTerm {
		rf.mu.Unlock()
		rf.becomeLeader()
	} else {
		rf.mu.Unlock()
	}
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	rf.setState(rfStateLeader)
	for i, _ := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log) + 1
	}
	for i, _ := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
	rf.mu.Unlock()
	rf.sendHeartbeats()
}

func (rf *Raft) gatherVotes() bool {
	rf.mu.Lock()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log),
		LastLogTerm:  rf.log.last().Term,
	}
	rf.mu.Unlock()

	mu := sync.Mutex{}
	cond := sync.NewCond(&mu)
	votes := 1
	finished := 0

	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}

		go func(server int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &args, &reply, rpcTimeout)
			mu.Lock()
			finished++
			if ok && reply.VoteGranted {
				votes++
			}
			rf.mu.Lock()
			if ok && reply.Term > rf.currentTerm {
				rf.setState(rfStateFollower)
				rf.currentTerm = reply.Term
			}
			rf.mu.Unlock()
			cond.Broadcast()
			mu.Unlock()
		}(server)
	}

	needVotes := len(rf.peers)/2 + 1
	mu.Lock()
	defer mu.Unlock()
	for votes < needVotes && finished < len(rf.peers)-1 {
		cond.Wait()
	}
	elected := votes >= needVotes
	return elected
}

func (rf *Raft) sendHeartbeats() {
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}

		go func(server int) {
			rf.mu.Lock()
			args := rf.getAppendEntriesArgs(server, true)
			rf.mu.Unlock()

			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(server, &args, &reply, rpcTimeout)

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if !ok {
				return
			}

			if rf.currentTerm < reply.Term {
				rf.setState(rfStateFollower)
				rf.currentTerm = reply.Term
			}

			if reply.Success {
				rf.matchIndex[server] = args.PrevLogIndex
				rf.nextIndex[server] = rf.matchIndex[server] + 1
			} else {
				rf.nextIndex[server]--
			}
		}(server)
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
	rf.peers = peers
	rf.persister = persister
	rf.applyCh = applyCh
	rf.me = me
	rf.electionTicker = NewRandomTicker(300*time.Millisecond, 600*time.Millisecond)
	rf.heartbeatTicker = time.NewTicker(110 * time.Millisecond)
	rf.logger = log.New(os.Stdout, "", 0)
	if !debug {
		rf.logger.SetOutput(ioutil.Discard)
	}
	rf.setState(rfStateFollower)
	rf.dead = make(chan struct{})

	// Your initialization code here (2C).
	// Initialize from persisted state
	rf.readPersist(persister.ReadRaftState())

	// Initialize volatile state
	rf.commitIndex = 0
	rf.lastApplied = 0

	// Initialize volatile leader state
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
