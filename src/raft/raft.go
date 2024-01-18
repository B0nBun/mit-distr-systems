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
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"io/ioutil"
)

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

type logEntry struct {
	command Command
	term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	logger          *log.Logger
	dead            chan struct{}
	electionTimer   *time.Timer
	heartbeatTicker *time.Ticker

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// Persistent state
	currentTerm int // Latest term seen (0 on first boot)
	votedFor    int // Peer that recieved vote in current term (-1 if none)
	log         []logEntry

	// Volatile state for all
	state       rfState // Current state of the server (follower, candidate, leader)
	commitIndex int     // Index of highest log entry known to be commited (0 initially)
	lastApplied int     // Index of highest log entry applied (0 initially)

	// Volatile state for leaders
	nextIndex  []int // For each peer, index of the next log entry to send (leader last log index + 1 initially)
	matchIndex []int // For each peer, index of the highest log entry known to be replicated (0 initially)
}

func (rf *Raft) String() string {
	return fmt.Sprintf("rf[i=%d,s=%s]", rf.me, rf.state)
}

func (rf *Raft) setState(state rfState) {
	rf.state = state
	rf.logger.SetPrefix(fmt.Sprintf("%v: ", rf))
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return rf.currentTerm, rf.state == rfStateLeader
}

func (rf *Raft) lastLogTerm() int {
	if len(rf.log) == 0 {
		return -1
	}
	return rf.log[len(rf.log)-1].term
}

func (rf *Raft) resetElectionTimer() bool {
	ms := 200 + (rand.Int63() % 300)
	rf.electionTimer.Stop()
	return rf.electionTimer.Reset(time.Duration(ms) * time.Millisecond)
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
	// Your data here (2A, 2B).
	Term         int // Candidate's term
	CandidateId  int // Candidate that is requesting a vote
	LastLogIndex int // Index of candidate's last log entry
	LastLogTerm  int // Term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // Term, for candidate to update intself to
	VoteGranted bool // Candidate recieved a vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func(reply *RequestVoteReply) {
		rf.logger.Printf("got RequestVote rpc from rf[i=%d] with term=%d. VoteGranted=%v", args.CandidateId, args.Term, reply.VoteGranted)
		if reply.VoteGranted {
			rf.resetElectionTimer()
			rf.votedFor = args.CandidateId
		}
	}(reply)

	canVote := rf.currentTerm != args.Term || rf.votedFor == -1 || rf.votedFor == args.CandidateId

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.setState(rfStateFollower)
	}

	reply.Term = rf.currentTerm
	lastLogTerm := rf.lastLogTerm()

	staleTerm := args.Term < rf.currentTerm
	staleLog := args.LastLogTerm < lastLogTerm
	if staleTerm || staleLog || !canVote {
		reply.VoteGranted = false
		return
	}

	upToDate := args.LastLogTerm == lastLogTerm && args.LastLogIndex >= len(rf.log)
	if upToDate {
		reply.VoteGranted = true
		return
	}
	reply.VoteGranted = false
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logger.Printf("got AppendEntries rpc from rf[i=%d]", args.LeaderId)
	reply.Term = rf.currentTerm
	reply.Success = args.Term < rf.currentTerm
	if args.Term >= rf.currentTerm {
		rf.resetElectionTimer()
	}
	if args.Term > rf.currentTerm {
		rf.setState(rfStateFollower)
		rf.currentTerm = args.Term
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, timeoutD time.Duration) bool {
	okC := make(chan bool)
	timeout := time.NewTimer(timeoutD)
	go func() {
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		okC<-ok
	}()
	select {
	case ok := <-okC:
		return ok
	case <-timeout.C:
		return false
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, timeoutD time.Duration) bool {
	okC := make(chan bool)
	timeout := time.NewTimer(timeoutD)
	go func() {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		okC<-ok
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
func (rf *Raft) Start(command Command) (int, int, bool) {
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
	// Your code here, if desired.
	rf.heartbeatTicker.Stop()
	rf.electionTimer.Stop()
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
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.resetElectionTimer()
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

	rf.logger.Printf("starting election with term=%d", rf.currentTerm)
	votes, maxTerm, ok := rf.gatherVotes()
	rf.logger.Printf("got %d votes (ok=%v) from election for term=%d", votes, ok, startingTerm)

	rf.mu.Lock()
	if maxTerm > rf.currentTerm {
		rf.setState(rfStateFollower)
		rf.currentTerm = maxTerm
		rf.mu.Unlock()
		return
	}
	stillCandidate := rf.state == rfStateCandidate
	elected := votes >= len(rf.peers)/2+1
	sameTerm := startingTerm == rf.currentTerm
	if ok && stillCandidate && elected && sameTerm {
		rf.mu.Unlock()
		rf.becomeLeader()
	} else {
		rf.mu.Unlock()
	}
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	rf.setState(rfStateLeader)
	rf.mu.Unlock()
	rf.sendHeartbeats()
}

func (rf *Raft) gatherVotes() (int, int, bool) {
	rf.mu.Lock()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log),
		LastLogTerm:  rf.lastLogTerm(),
	}
	rf.mu.Unlock()

	// Struct shared between goroutines
	var shared struct {
		mu sync.Mutex
		votes int
		maxTerm int
		ok bool
	}
	shared.votes = 1
	shared.maxTerm = rf.currentTerm
	shared.ok = true

	var wg sync.WaitGroup
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}

		wg.Add(1)
		go func(server int) {
			defer wg.Done()
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &args, &reply, rpcTimeout)
			shared.mu.Lock()
			if ok && reply.VoteGranted {
				shared.votes += 1
			}
			if ok && reply.Term > shared.maxTerm {
				shared.maxTerm = reply.Term
			}
			if !ok {
				shared.ok = false
			}
			shared.mu.Unlock()
		}(server)
	}

	wg.Wait()

	return shared.votes, shared.maxTerm, shared.ok
}

func (rf *Raft) sendHeartbeats() {
	rf.logger.Printf("sending heartbeats")
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	var (
		maxTermMu sync.Mutex
		maxTerm int = rf.currentTerm
		wg sync.WaitGroup
	)
	rf.mu.Unlock()
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		
		wg.Add(1)
		go func(server int) {
			defer wg.Done()
			reply := AppendEntriesReply{}
			rf.sendAppendEntries(server, &args, &reply, rpcTimeout)
			maxTermMu.Lock()
			if maxTerm < reply.Term {
				maxTerm = reply.Term
			}
			maxTermMu.Unlock()
		}(server)
	}
	wg.Wait()

	rf.mu.Lock()
	if maxTerm > rf.currentTerm {
		rf.setState(rfStateFollower)
		rf.currentTerm = maxTerm
	}
	rf.mu.Unlock()
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
	rf.electionTimer = time.NewTimer(time.Minute)
	rf.electionTimer.Stop()
	rf.resetElectionTimer()
	rf.heartbeatTicker = time.NewTicker(110 * time.Millisecond)
	rf.state = rfStateFollower
	rf.logger = log.New(os.Stdout, fmt.Sprintf("%v: ", rf), 0)
	if !debug {
		rf.logger.SetOutput(ioutil.Discard)
	}
	rf.votedFor = -1
	rf.dead = make(chan struct{})

	// Your initialization code here (2A, 2B, 2C).
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
