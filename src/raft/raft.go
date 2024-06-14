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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"log"
	"os"
	"fmt"
	"io/ioutil"

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
	Term int
}

type raftState string

const (
	follower raftState = "f"
	candidate = "c"
	leader = "l"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	l         *log.Logger

	// Your data here (2B, 2C).

	// Persistent state
	currentTerm int
	votedFor int
	log []LogEntry

	// Volatile state for all servers
	state raftState
	commitIndex int
	lastApplied int
	electionTicker *RandomTicker
	heartbeatTicker *time.Ticker

	// Volatile state for leaders
	nextIndex []int
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
		return rf.log[len(rf.log) - 1].Term
	}
	return -1
}

func (rf *Raft) resetToFollower(newTerm int) {
	rf.state = follower
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
	// Your data here (2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2B).
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
	upToDate := args.LastLogTerm >= rf.lastLogTerm() && args.LastLogIndex >= len(rf.log) - 1
	canVote := rf.votedFor == -1 || rf.votedFor == args.CandidateId 
	if canVote && upToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.electionTicker.Reset()
	}
}

type AppendEntriesArgs struct {
	Term int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.l.Printf("got AppendEntries %+v", args)
	defer rf.l.Printf("respond to AppendEntries %+v", reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	if rf.currentTerm < args.Term {
		rf.resetToFollower(args.Term)
	}
	reply.Success = true
	rf.electionTicker.Reset()
	return
	// TODO (2B): Rest
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if len(args.Entries) > 0 {
		rf.l.Printf("sendAppendEntries to %d %+v ", server, args)
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) startElection() {
	rf.l.Printf("start election")
	rf.mu.Lock()
	rf.currentTerm ++
	electionTerm := rf.currentTerm
	rf.votedFor = rf.me
	args := RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm: rf.lastLogTerm(),
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
			finished ++
			if ok && reply.VoteGranted {
				votes ++
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
	for votes < needVotes && finished < len(rf.peers) - 1 {
		cond.Wait()
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	sameTerm := rf.currentTerm == electionTerm && rf.state == candidate
	elected := votes >= needVotes
	if sameTerm && elected {
		rf.state = leader
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


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
	rf.electionTicker.Stop()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) sendHeartbeats() {
	rf.l.Printf("send heartbeats")
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term: rf.currentTerm,
		PrevLogIndex: len(rf.log) - 1,
		PrevLogTerm: rf.lastLogTerm(),
		Entries: make([]LogEntry, 0),
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			reply := AppendEntriesReply{}
			rf.sendAppendEntries(server, &args, &reply)
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.resetToFollower(reply.Term)
			}
			rf.mu.Unlock()
		}(server)
	}
}

func (rf *Raft) heartbeat() {
	for {
		<-rf.heartbeatTicker.C
		rf.mu.Lock()
		if rf.state != leader {
			rf.mu.Unlock()
			continue
		} else {
			rf.mu.Unlock()
		}
		go rf.sendHeartbeats()
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		select {
		case <-rf.electionTicker.C:
			rf.mu.Lock()
			if rf.state != leader {
				rf.l.Printf("election time-out")
				rf.state = candidate
				go rf.startElection()
			}
			rf.mu.Unlock()
		default:
			// pass
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
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
	rf.l = log.New(os.Stderr, fmt.Sprintf("rf[%d]", rf.me), 0)
	raftLogs := os.Getenv("RAFT_LOGS") == "true"
	if !raftLogs {
		rf.l.SetOutput(ioutil.Discard)
	}
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.state = follower
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.electionTicker = NewRandomTicker(200 * time.Millisecond, 400 * time.Millisecond)
	rf.heartbeatTicker = time.NewTicker(50 * time.Millisecond)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i, _ := range rf.peers {
		rf.nextIndex[i] = -1
		rf.matchIndex[i] = -1
	}


	// Your initialization code here (2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartbeat()


	return rf
}
