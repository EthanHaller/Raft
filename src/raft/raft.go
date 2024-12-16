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
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

    applyCh     chan ApplyMsg

	state		string
	currentTerm int
	votedFor    int
	log			[]logEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	lastHeartbeat time.Time
}

type logEntry struct {
	Term	int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.state == "leader"

	return term, isleader
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Function skeleton for log persistence.
	// **Not required** for CS4740 Fall24 Lab3. You may skip.
	// (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return	
	}
	// Function skeleton for log persistence.
	// **Not required** for CS4740 Fall24 Lab3. You may skip.
	// (3C).
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

type AppendEntriesArgs struct {
	Term 		 int
	LeaderId 	 int
	PrevLogIndex int
	PrevLogTerm  int
	Entries		 []logEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term	int
	Success bool
}

// Handler for AppendEntries invoked by the leader to replicate log entries. If args.Entries is nil, this is used to handle
// heartbeats.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    reply.Term = rf.currentTerm

    if args.Term < rf.currentTerm {
        reply.Success = false
        return
    }

    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        rf.votedFor = -1
        rf.state = "follower"
    }
    rf.lastHeartbeat = time.Now()

    if args.PrevLogIndex >= len(rf.log) || (args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
        reply.Success = false
        return
    }

    rf.log = rf.log[:args.PrevLogIndex+1]
    rf.log = append(rf.log, args.Entries...)

    if args.LeaderCommit > rf.commitIndex {
        rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
        go rf.updateCommitIndex()
    }

    reply.Success = true
}

// Sends an AppendEntries RPC to a given server
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term 		 int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (3A).
	Term 		int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
    defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = "follower"
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && isLogUpToDate(rf, args) {
		rf.lastHeartbeat = time.Now()
        reply.VoteGranted = true
        rf.votedFor = args.CandidateId
    } else {
        reply.VoteGranted = false
    }

    reply.Term = rf.currentTerm
}

// Checks if the log belonging to args is up to date with rf.log
func isLogUpToDate(rf *Raft, args *RequestVoteArgs) bool {
    if len(rf.log) == 0 {
        return true
    }

    lastLogIndex := len(rf.log) - 1
    lastLogTerm := rf.log[lastLogIndex].Term

    if args.LastLogTerm != lastLogTerm {
        return args.LastLogTerm > lastLogTerm
    }
    return args.LastLogIndex >= lastLogIndex
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Will start the election to be a leader and send RequestVote RPCs to peer servers
func (rf *Raft) startElection() {
    rf.state = "candidate"
    rf.currentTerm++
    rf.votedFor = rf.me
    votes := 1

    lastLogIndex := len(rf.log) - 1
    lastLogTerm := 0
    if lastLogIndex >= 0 {
        lastLogTerm = rf.log[lastLogIndex].Term
    }

    args := RequestVoteArgs{
        Term:         rf.currentTerm,
        CandidateId:  rf.me,
        LastLogIndex: lastLogIndex,
        LastLogTerm:  lastLogTerm,
    }

    for i := range rf.peers {
        if i == rf.me {
            continue
        }
        go func(server int) {
            reply := RequestVoteReply{}
            if rf.sendRequestVote(server, &args, &reply) {
                rf.mu.Lock()
                defer rf.mu.Unlock()

                if reply.VoteGranted {
                    votes++
                    if votes > len(rf.peers)/2 && rf.state == "candidate" {
                        rf.becomeLeader()
                    }
                } else if reply.Term > rf.currentTerm {
                    rf.currentTerm = reply.Term
                    rf.state = "follower"
                    rf.votedFor = -1
                }
            }
        }(i)
    }

	go func() {
        time.Sleep(time.Duration(rand.Intn(750)+150) * time.Millisecond)
        rf.mu.Lock()
        defer rf.mu.Unlock()

        if rf.state == "candidate" {
            rf.startElection()
        }
    }()
}

// As a candidate, become the leader
func (rf *Raft) becomeLeader() {
    rf.state = "leader"

    for i := range rf.peers {
        rf.nextIndex[i] = len(rf.log)
        rf.matchIndex[i] = 0
    }

    go func() {
        for rf.state == "leader" {
            rf.sendHeartbeats()

            time.Sleep(150 * time.Millisecond)
        }
    }()
}

// This function will periodically send heartbeats to followers. If Entries is not empty, then AppendEntries will handle any
// new entries
// If followers successfully append the entries to their logs, then this will send another AppendEntries so followers will
// update their commitIndex and apply messages
// If the next index of the follower does not match, try again with the known previous log index
func (rf *Raft) sendHeartbeats() {
    for server := range rf.peers {
        if server != rf.me {
            go func() {
                rf.mu.Lock()

                args := AppendEntriesArgs{
                    Term:         rf.currentTerm,
                    LeaderId:     rf.me,
                    LeaderCommit: rf.commitIndex,
                }

                args.PrevLogIndex = rf.nextIndex[server] - 1
                if args.PrevLogIndex >= 0 {
                    args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
                }
                args.Entries = rf.log[rf.nextIndex[server]:]
        
                rf.mu.Unlock()
        
                reply := AppendEntriesReply{}
                ok := rf.sendAppendEntries(server, &args, &reply)

                rf.mu.Lock()
                if !ok || rf.state != "leader" {
                    rf.mu.Unlock()
                    return
                }

                if reply.Term > rf.currentTerm {
                    rf.currentTerm = reply.Term
                    rf.state = "follower"
                    rf.votedFor = -1
                    rf.mu.Unlock()
                    return
                }

                if reply.Success {
                    prevMatchIndex := rf.matchIndex[server]
                    rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
                    rf.nextIndex[server] = rf.matchIndex[server] + 1

                    if rf.matchIndex[server] > prevMatchIndex {
                        rf.sendHeartbeats()
                        rf.updateCommitIndex()
                    }
                } else {
                    rf.nextIndex[server] = args.PrevLogIndex
                    rf.sendHeartbeats()
                }
                rf.mu.Unlock()
            }()
        }
    }
}

// Update the commit index to be the latest log index known by all followers
func (rf *Raft) updateCommitIndex() {
    N := rf.commitIndex + 1
    for ; N < len(rf.log); N++ {
        matchCount := 1

        for i := range rf.peers {
            if i != rf.me && rf.matchIndex[i] >= N {
                matchCount++
            }
        }

        if matchCount > len(rf.peers)/2 && rf.log[N].Term == rf.currentTerm {
            rf.commitIndex = N
        }
    }

    rf.applyCommittedEntries()
}

// Apply all log entries that have been committed but not yet applied
func (rf *Raft) applyCommittedEntries() {
    for rf.lastApplied < rf.commitIndex {
        rf.lastApplied++
        applyMsg := ApplyMsg{
            CommandValid: true,
            Command:      rf.log[rf.lastApplied].Command,
            CommandIndex: rf.lastApplied,
        }
        rf.applyCh <- applyMsg
    }
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

	term := rf.currentTerm
	isLeader := rf.state == "leader"

    if !isLeader {
        return -1, term, isLeader
    }

    rf.log = append(rf.log, logEntry{Term: term, Command: command})

    rf.nextIndex[rf.me] = len(rf.log)
    rf.matchIndex[rf.me] = len(rf.log) - 1
    index := len(rf.log) - 1

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

    rf.applyCh = applyCh

	rf.state = "follower"
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.log = make([]logEntry, 0)
    rf.log = append(rf.log, logEntry{Term: -1, Command: nil})

	rf.commitIndex = 0
    rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
    rf.matchIndex = make([]int, len(peers))

    for i := range rf.matchIndex {
        rf.matchIndex[i] = -1
    }

	rf.lastHeartbeat = time.Now()

	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {
			timeout := time.Duration(rand.Intn(750)+150) * time.Millisecond
			time.Sleep(timeout)
	
			rf.mu.Lock()
			if time.Since(rf.lastHeartbeat) >= timeout && rf.state == "follower" {
				rf.startElection()
			}
			rf.mu.Unlock()
			
			time.Sleep(10 * time.Millisecond)
		}
	}()	

	return rf
}
