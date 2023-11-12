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
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
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

const (
	Leader int = iota
	Candidate
	Follower
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// Your data here (2A, 2B, 2C).
	// 2A
	currentTerm          int
	votedFor             int
	electionTimerResetAt time.Time
	electionTimeout      time.Duration
	IAm                  int
	// 2B
	log []LogEntry
	// others
	electionTimerLock sync.Mutex
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	Lg(rf.me, dTerm, fmt.Sprintf("term: %v, isleader: %v", rf.currentTerm, rf.IAm == Leader))
	term = rf.currentTerm
	isleader = rf.IAm == Leader
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
// func (rf *Raft) persist() {
// 	// Your code here (2C).
// 	// Example:
// 	// w := new(bytes.Buffer)
// 	// e := labgob.NewEncoder(w)
// 	// e.Encode(rf.xxx)
// 	// e.Encode(rf.yyy)
// 	// raftstate := w.Bytes()
// 	// rf.persister.Save(raftstate, nil)
// }

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
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type LogEntry struct {
	Term    int
	Command interface{}
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

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	Lg(rf.me, dVote, "I was asked to vote for Term %v, candidate %v", args.Term, args.CandidateId)

	// if asking vote for previous term
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		Lg(rf.me, dVote, "Term %v < currentTerm %v", args.Term, rf.currentTerm)
		rf.mu.Unlock()
		return
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		rf.electionTimerResetAt = time.Now()
		rf.IAm = Follower
		reply.VoteGranted = true
		reply.Term = args.Term
		Lg(rf.me, dVote, "Voted for %v", args.CandidateId)
		rf.mu.Unlock()
		return
	}

	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	Lg(rf.me, dVote, "Already voted for %v for term %v", rf.votedFor, rf.currentTerm)
	rf.mu.Unlock()
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.ReplyAppendEntries", args, reply)
	return ok
}

func (rf *Raft) ReplyAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	Lg(rf.me, dLog, "Received AppendEntries from %v", args.LeaderId)
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		Lg(rf.me, dLog, "Reject append entry Term %v < currentTerm %v", args.Term, rf.currentTerm)
		return
	}
	// TODO: Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm

	// TODO: If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it

	// Append any new entries not already in the log
	rf.log = append(rf.log, args.Entries...)

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	// if args.LeaderCommit > rf.commitIndex {
	// 	rf.commitIndex = args.LeaderCommit
	// }

	// 	If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.IAm = Follower
		rf.votedFor = -1
	}

	rf.mu.Unlock()
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
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {

	for !rf.killed() {
		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if time.Since(rf.electionTimerResetAt) > rf.electionTimeout {
			Lg(rf.me, dTimer, "Election timeout, starting election")
			Lg(rf.me, dTimer, "Locking for election params")
			rf.IAm = Candidate
			// Increment current term
			rf.currentTerm += 1
			// vote for self
			numVotes := atomic.Int32{}
			numVotes.Add(1)
			rf.votedFor = rf.me
			// Reset election timer
			rf.electionTimerResetAt = time.Now()

			// Send RequestVote RPCs to all other servers
			args := RequestVoteArgs{}
			args.Term = rf.currentTerm
			args.CandidateId = rf.me
			args.LastLogIndex = 0
			args.LastLogTerm = 0

			rf.mu.Unlock()
			var nextTerm int

			waitGrp := sync.WaitGroup{}
			for server := range rf.peers {
				waitGrp.Add(1)
				go func(server int) {
					if server == rf.me {
						waitGrp.Done()
						return
					}
					repl := RequestVoteReply{}
					rf.sendRequestVote(server, &args, &repl)
					Lg(rf.me, dTimer, "Received vote from %v: %v for term %v", server, repl.VoteGranted, repl.Term)
					if repl.VoteGranted && repl.Term == rf.currentTerm {
						numVotes.Add(1)
					} else {
						nextTerm = repl.Term
					}
					waitGrp.Done()
				}(server)
			}
			// wait for all replies
			Lg(rf.me, dTimer, "Waiting for election results")
			waitGrp.Wait()
			Lg(rf.me, dTimer, "Election results received")
			// If votes received from majority of servers: become leader
			// If AppendEntries RPC received from new leader: convert to follower
			rf.mu.Lock()
			Lg(rf.me, dTimer, "Locking for election results")
			if rf.currentTerm < nextTerm {
				rf.currentTerm = nextTerm
				rf.votedFor = -1
				rf.IAm = Follower
				Lg(rf.me, dTimer, "Becoming follower %v", rf.currentTerm)
			}
			if rf.IAm == Candidate && int(numVotes.Load()) > len(rf.peers)/2 {
				rf.IAm = Leader
				Lg(rf.me, dTimer, "Becoming leader %v", rf.currentTerm)

				// Upon election: send initial empty AppendEntries RPCs
				// (heartbeat) to each server; repeat during idle periods
				// to prevent election timeouts (§5.2)

				appendEntriesArgs := AppendEntriesArgs{}
				appendEntriesArgs.Term = rf.currentTerm
				appendEntriesArgs.LeaderId = rf.me
				appendEntriesArgs.PrevLogIndex = 0
				appendEntriesArgs.PrevLogTerm = 0
				appendEntriesArgs.Entries = []LogEntry{}
				appendEntriesArgs.LeaderCommit = 0

				for server := range rf.peers {
					if server == rf.me {
						continue
					}
					go func(server int) {
						repl := AppendEntriesReply{}
						rf.sendAppendEntries(server, &appendEntriesArgs, &repl)
					}(server)
				}
			}

			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
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
	debugInit()
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.mu.Lock()
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.electionTimerResetAt = time.Now()
	rf.electionTimeout = 500 * time.Millisecond
	rf.IAm = Follower
	rf.log = []LogEntry{}
	rf.mu.Unlock()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// __________________________________________________________________________________________
// Logger helper
// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	// log.Printf("Verbosity level %v", level)
	return level
}

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

var debugStart time.Time
var debugVerbosity int

func debugInit() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Lg(me int, topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefixSpaced := ""
		prefixSpaces := 40
		for i := 0; i < me*prefixSpaces; i++ {
			prefixSpaced += " "
		}
		prefix := fmt.Sprintf("%s %06d S%d %v ", prefixSpaced, time, me, string(topic))

		format = prefix + format
		log.Printf(format, a...)
	}
}
