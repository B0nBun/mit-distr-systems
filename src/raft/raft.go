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
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

type Command interface{}

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

type raftState int32

const (
	follower  raftState = 1
	candidate           = 2
	leader              = 3
)

func (s *raftState) load() raftState {
	return raftState(atomic.LoadInt32((*int32)(s)))
}

func (s *raftState) store(val raftState) {
	atomic.StoreInt32((*int32)(s), int32(val))
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	deadC     chan struct{}
	applyCh   chan ApplyMsg
	cond      *sync.Cond
	l         *log.Logger

	// Your data here (2C).

	// Persistent state
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state for all servers
	state           raftState
	commitIndex     int
	lastApplied     int
	electionTicker  *RandomTicker
	heartbeatTicker *time.Ticker

	// Volatile state for leaders
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == leader
}

func (rf *Raft) lastLogTerm() int {
	if len(rf.log) > 0 {
		return rf.log[len(rf.log)-1].Term
	}
	return -1
}

func (rf *Raft) resetToFollower(newTerm int) {
	rf.state.store(follower)
	rf.currentTerm = newTerm
	rf.votedFor = -1
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
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
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
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.l.Printf("got RequestVote %+v", args)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.l.Printf("respond to RequestVote: %+v", reply)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	if rf.currentTerm < args.Term {
		rf.resetToFollower(args.Term)
	}
	lastLogTerm := rf.lastLogTerm()
	var upToDate bool
	if args.LastLogTerm != lastLogTerm {
		upToDate = args.LastLogTerm > lastLogTerm
	} else {
		upToDate = args.LastLogIndex >= len(rf.log)-1
	}
	canVote := rf.votedFor == -1 || rf.votedFor == args.CandidateId
	if canVote && upToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.electionTicker.Reset()
	}
}

type AppendEntriesArgs struct {
	Term         int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// For faster backup
	XTerm  int
	XIndex int
	XLen   int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.l.Printf("got AppendEntries %+v", args)
	rf.mu.Lock()
	defer rf.l.Printf("respond to AppendEntries %+v", reply)
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.XLen = len(rf.log)
	reply.XTerm = -1
	reply.XIndex = -1
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	if rf.currentTerm < args.Term {
		rf.resetToFollower(args.Term)
	}
	if len(rf.log)-1 < args.PrevLogIndex {
		reply.Success = false
		return
	}
	if args.PrevLogIndex != -1 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.XTerm = rf.log[args.PrevLogIndex].Term
		reply.XIndex = args.PrevLogIndex
		for reply.XIndex-1 >= 0 && rf.log[reply.XIndex-1].Term == reply.XTerm {
			reply.XIndex--
		}
		return
	}
	reply.Success = true
	if rf.state == candidate {
		rf.state.store(follower)
	}
	rf.electionTicker.Reset()

	if len(args.Entries) != 0 {
		// NOTE: Not the most effective solution, but the easiest
		rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
		rf.cond.Broadcast()
		rf.l.Printf("broadcast log changes to replicator (AppendEntries)")
	}
	lastNewEntry := len(rf.log) - 1
	if args.LeaderCommit > rf.commitIndex {
		rf.commitUpTo(minimum(args.LeaderCommit, lastNewEntry))
	}
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
	rf.l.Printf("sendRequestVote to %d %+v", server, args)
	okC := make(chan bool)
	go func() {
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		okC <- ok
	}()
	select {
	case ok := <-okC:
		return ok
	case <-rf.deadC:
		return false
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if len(args.Entries) > 0 {
		rf.l.Printf("sendAppendEntries to %d %+v ", server, args)
	}
	okC := make(chan bool)
	go func() {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		okC <- ok
	}()
	select {
	case ok := <-okC:
		return ok
	case <-rf.deadC:
		return false
	}
}

func (rf *Raft) startElection() {
	rf.l.Printf("start election")
	rf.mu.Lock()
	rf.state.store(candidate)
	rf.currentTerm++
	electionTerm := rf.currentTerm
	rf.votedFor = rf.me
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.lastLogTerm(),
	}
	rf.electionTicker.Reset()
	rf.mu.Unlock()
	mu := sync.Mutex{}
	cond := sync.NewCond(&mu)
	votes := 1
	finished := 0
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &args, &reply)
			mu.Lock()
			finished++
			if ok && reply.VoteGranted {
				votes++
			}
			rf.mu.Lock()
			if ok && reply.Term > rf.currentTerm {
				rf.resetToFollower(reply.Term)
				rf.mu.Unlock()
				return
			} else {
				rf.mu.Unlock()
			}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	sameTerm := rf.currentTerm == electionTerm && rf.state == candidate
	elected := votes >= needVotes
	if sameTerm && elected {
		rf.state.store(leader)
		rf.cond.Broadcast()
		rf.l.Printf("broadcast state change to replicator")
		for i, _ := range rf.peers {
			rf.nextIndex[i] = len(rf.log)
			rf.matchIndex[i] = -1
		}
		rf.cond.Broadcast()
		rf.l.Printf("broadcast nextIndex changes to replicator")
		go rf.sendHeartbeats()
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
func (rf *Raft) Start(command Command) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.log) + 1
	term := rf.currentTerm
	isLeader := rf.state == leader
	rf.l.Printf("start(%+v) -> (%d, %d, %v)", command, index, term, isLeader)
	if isLeader {
		rf.log = append(rf.log, LogEntry{Command: command, Term: term})
		rf.cond.Broadcast()
		rf.l.Printf("broadcast log changes to replicator (Start)")
	}
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
	close(rf.deadC)
	rf.electionTicker.Stop()
	rf.heartbeatTicker.Stop()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) commitUpTo(index int) {
	if index > rf.commitIndex {
		rf.commitIndex = index
		rf.l.Printf("commitIndex = %d", rf.commitIndex)
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied + 1,
				// TODO: Snapshots and staff (2D)
			}
			rf.l.Printf("applying %+v", msg)
			rf.applyCh <- msg
		}
	}
}

func (rf *Raft) sendHeartbeats() {
	rf.l.Printf("send heartbeats")
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		PrevLogIndex: len(rf.log) - 1,
		PrevLogTerm:  rf.lastLogTerm(),
		Entries:      make([]LogEntry, 0),
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(server, &args, &reply)
			rf.mu.Lock()
			if ok && reply.Term > rf.currentTerm {
				rf.resetToFollower(reply.Term)
			} else if ok && !reply.Success {
				rf.nextIndex[server] = minimum(rf.nextIndex[server], args.PrevLogIndex)
			}
			rf.mu.Unlock()
		}(server)
	}
}

func (rf *Raft) replicateLogs(server int) {
	rf.mu.Lock()
	rf.l.Printf("start replicating logs for server=%d", server)
	defer func() {
		rf.l.Printf("finished replicating logs server=%d (nextIndex[%d]=%d)", server, server, rf.nextIndex[server])
	}()
	defer rf.mu.Unlock()
	startTerm := rf.currentTerm
	for len(rf.log)-1 >= rf.nextIndex[server] && !rf.killed() {
		if rf.currentTerm != startTerm || rf.state != leader {
			break
		}
		untilIndex := len(rf.log) - 1
		newEntries := rf.log[rf.nextIndex[server] : untilIndex+1]
		prevLogIndex := rf.nextIndex[server] - 1
		prevLogTerm := -1
		if prevLogIndex >= 0 {
			prevLogTerm = rf.log[prevLogIndex].Term
		}
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      newEntries,
			LeaderCommit: rf.commitIndex,
		}
		reply := AppendEntriesReply{}
		rf.mu.Unlock()
		ok := rf.sendAppendEntries(server, &args, &reply)
		rf.mu.Lock()
		if !ok {
			continue
		}
		if reply.Term > args.Term {
			rf.resetToFollower(reply.Term)
			break
		}
		if reply.Success {
			rf.nextIndex[server] = untilIndex + 1
			rf.matchIndex[server] = untilIndex
		} else if reply.XTerm == -1 {
			rf.nextIndex[server] = reply.XLen
		} else {
			var i int
			for i = untilIndex; i >= 0; i-- {
				if rf.log[i].Term == reply.XTerm {
					break
				} else if rf.log[i].Term < reply.XTerm {
					i = -1
					break
				}
			}
			if i == -1 {
				rf.nextIndex[server] = reply.XIndex
			} else {
				rf.nextIndex[server] = i
			}
		}
	}
}

// NOTE: rf.mu must be rf.cond's "locker" for this to work
func (rf *Raft) replicator(server int) {
	for !rf.killed() {
		rf.mu.Lock()
		for len(rf.log)-1 < rf.nextIndex[server] || rf.state != leader {
			rf.cond.Wait()
		}
		rf.mu.Unlock()
		rf.replicateLogs(server)
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		for n := len(rf.log) - 1; n > rf.commitIndex; n-- {
			if rf.log[n].Term != rf.currentTerm {
				continue
			}
			matched := 1
			for server := range rf.matchIndex {
				if server == rf.me {
					continue
				}
				if rf.matchIndex[server] >= n {
					matched++
				}
			}
			if matched >= len(rf.peers)/2+1 {
				rf.commitUpTo(n)
				break
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionTicker.C:
			state := rf.state.load()
			if state != leader {
				rf.l.Printf("election time-out")
				go rf.startElection()
			}
		case <-rf.heartbeatTicker.C:
			state := rf.state.load()
			if state != leader {
				continue
			}
			go rf.sendHeartbeats()
		case <-rf.deadC:
			return
		}
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
	rf.me = me
	rf.dead = 0
	rf.deadC = make(chan struct{})
	rf.applyCh = applyCh
	rf.cond = sync.NewCond(&rf.mu)
	rf.l = log.New(os.Stderr, fmt.Sprintf("rf[%d]: ", rf.me), 0)
	raftLogs := os.Getenv("RAFT_LOGS") == "true"
	if !raftLogs {
		rf.l.SetOutput(ioutil.Discard)
	}
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.state.store(follower)
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.electionTicker = NewRandomTicker(200*time.Millisecond, 400*time.Millisecond)
	rf.heartbeatTicker = time.NewTicker(75 * time.Millisecond)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i, _ := range rf.peers {
		rf.nextIndex[i] = 0
		rf.matchIndex[i] = -1
	}

	// Your initialization code here (2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go rf.replicator(server)
	}

	return rf
}
