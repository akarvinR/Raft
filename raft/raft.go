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
	"cs651/labrpc"
	"sort"
	"bytes"
	"cs651/labgob"
	"sync"
	"sync/atomic"
	"time"
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu          sync.Mutex // Lock to protect shared access to this peer's state
	termLocker sync.Mutex
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	dead        int32               // set by Kill()
	randomMilliseconds int
	ApplyMsgChn chan ApplyMsg
	currentTerm int
	votedFor    int
	log         []LogItem

	votesReceived int32
	commitIndex   int
	lastApplied   int

	nextIndex  []int
	matchIndex []int

	State string

	electionTimeOut *time.Ticker
}

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

type LogItem struct {
	Term         int
	Command      interface{}
	LogItemIndex int
}

// return currentTerm and whether this server
// believes it is the leader.time.NewTicker

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.SaveStateAndSnapshot() or use persister.SaveRaftState().
// after you've implemented snapshots, pass the current snapshot to persister.SaveStateAndSnapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)
	e.Encode(rf.votedFor)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.log)
	d.Decode(&rf.votedFor)

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
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	term = rf.currentTerm
	isleader = (rf.State == "leader")
	return term, isleader
}

func (rf *Raft) changeState(to string) {
	if to == "leader" {
		print("**************************************************8Leader elected ", rf.me, "\n")
		rf.State = "leader"
		rf.stopElectionTimeOut()
		rf.initializeNextIndex()
		go rf.sendHeartBeat()
		go rf.commitIndexListener()
		go rf.initializeLogListener()
	} else if to == "candidate" {
		rf.State = "candidate"
	} else {
		rf.State = "follower"
		rf.stopElectionTimeOut()
		rf.startElectionTimeOut()
	}
}

func (rf *Raft) initializeNextIndex() {
	rf.mu.Lock()
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}
	rf.mu.Unlock()
}
func (rf *Raft) sendHeartBeat() {
	for rf.State == "leader" && !rf.killed() {
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go rf.sendAppendEntries(i)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term < rf.currentTerm || rf.State == "leader" {
		reply.Term = rf.currentTerm
		reply.WrongLog = false
		reply.VoteGranted = false
	} else {
		rf.changeTerm(args.Term)
	
		if (rf.votedFor == -1 || (rf.votedFor == args.CandidateId))  && rf.isMoreUpdateToDate(args.LastLogTerm, args.LastLogIndex, rf.log[len(rf.log)-1].Term, len(rf.log)) {
			print(rf.me, " received  request vote request from ", args.CandidateId, " ", "My Term ", rf.currentTerm, " Voter Term", args.Term, " votedFor", rf.votedFor, "\n")
			reply.Term = rf.currentTerm
			reply.VoteGranted = true

			rf.votedFor = args.CandidateId
		} else {
			print(rf.me, " (voted no) received  request vote request from ", args.CandidateId, " ", "My Term ", rf.currentTerm, " Voter Term", args.Term, " votedFor", rf.votedFor, "\n")
			
			reply.Term = rf.currentTerm
			reply.WrongLog =  !rf.isMoreUpdateToDate(args.LastLogTerm, args.LastLogIndex, rf.log[len(rf.log)-1].Term, len(rf.log))		
			reply.VoteGranted = false
		
		}
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

func (rf *Raft) sendAppendEntries(server int) bool {
	rf.mu.Lock()
	if rf.State != "leader" {
		rf.mu.Unlock()
		return false
	}
	appendEntriesArgs := AppendEntriesArgs{}
	appendEntriesReply := AppendEntriesReply{}
	appendEntriesArgs.Term = rf.currentTerm
	appendEntriesArgs.LeaderId = rf.me

	nextIndexValue := rf.nextIndex[server]
	lengthOfLog := len(rf.log)
	appendEntriesArgs.PrevLogIndex = min(nextIndexValue-1, lengthOfLog-1)
	PrevLogTerm := rf.log[appendEntriesArgs.PrevLogIndex].Term
	entries := rf.log[nextIndexValue:]

	appendEntriesArgs.PrevLogTerm = PrevLogTerm
	appendEntriesArgs.Entries = entries
	appendEntriesArgs.LeaderCommit = rf.commitIndex
	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.AppendEntries", &appendEntriesArgs, &appendEntriesReply)
	if !ok {
		return false
	}

	rf.mu.Lock()
	if appendEntriesReply.Success {

		if len(entries) > 0 {
			rf.nextIndex[server] = nextIndexValue + len(entries)
			rf.matchIndex[server] = rf.nextIndex[server] - 1
			
		}
	} else {
		if appendEntriesReply.Term > rf.currentTerm {
			print("Leader Stepping down ", rf.me, " with new term", rf.currentTerm, "\n")
			rf.changeState("follower")
			rf.changeTerm(appendEntriesReply.Term)

			
		} else if appendEntriesReply.IsError {
			if appendEntriesReply.ErrorLength <= appendEntriesArgs.PrevLogIndex {
				rf.nextIndex[server] = appendEntriesReply.ErrorLength
			} else {
				rf.nextIndex[server] = appendEntriesReply.ErrorIndex
				for i := 0; i < len(rf.log); i++ {
					if rf.log[i].Term == appendEntriesReply.ErrorLength {
						rf.nextIndex[server] = i
						break
					}
				}
			}
		}
	}
	rf.persist()
	
	rf.mu.Unlock()
	return ok
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

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	index := -1
	term := -1
	isLeader := (rf.State == "leader")

	if !isLeader {
		rf.mu.Unlock()
		return index, term, isLeader
	} else {


		rf.log = append(rf.log, LogItem{Term: rf.currentTerm, Command: command, LogItemIndex: len(rf.log)})
		// print(len(rf.log))
		index = len(rf.log) - 1
		term = rf.currentTerm
		// print("Start ", rf.me, " ", command.(int), " index:", index, "\n")
		rf.persist()
		rf.mu.Unlock()
		return index, term, isLeader
	}

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
	print(rf.me, " is killed ", rf.State, " term ", rf.currentTerm, "\n")
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func min(x int, y int) int {
	if x < y {
		return x
	}
	return y
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	currentTerm := rf.currentTerm
	if rf.State == "leader" {
		if args.Term >= currentTerm { //TODO CHANGE THIS
			print("Leader Stepping down ", rf.me, " with new term", rf.currentTerm, "\n")
			reply.Term = currentTerm
			reply.Success = false
			reply.IsError = false
			rf.changeState("follower")
			rf.changeTerm(args.Term)
			if args.Term > currentTerm {
				rf.votedFor = -1
			}
		} else if args.Term < rf.currentTerm {
			reply.Success = false
			reply.Term = args.Term
		}

	} else if rf.State == "candidate" {

		if args.Term >= currentTerm {
			rf.changeState("follower")
			reply.Term = currentTerm
			reply.Success = false
			reply.IsError = false
			rf.changeTerm(args.Term)
		} else if args.Term < currentTerm {
			reply.Success = false
			reply.IsError = false
			reply.Term = rf.currentTerm
		}
	} else if rf.State == "follower" {
		if args.Term >= currentTerm {
			rf.resetElectionTimeOut()
			rf.changeTerm(args.Term)

			if args.PrevLogIndex < len (rf.log) && args.PrevLogTerm == rf.log[args.PrevLogIndex].Term {
				reply.Success = true
				reply.Term = currentTerm
				reply.IsError = false
				rf.log = rf.log[:args.PrevLogIndex+1]
				rf.log = append(rf.log, args.Entries...)
				rf.commitEntries(min(args.LeaderCommit, len(rf.log)-1))
			}else{
				reply.Success = false
				reply.Term = rf.currentTerm
				reply.IsError = true
				reply.ErrorLength= len(rf.log)
				if(args.PrevLogIndex < len(rf.log)){
					reply.ErrorTerm = rf.log[args.PrevLogIndex].Term
					for i:=args.PrevLogIndex;i>=0;i-- {
						if(rf.log[i].Term != reply.ErrorTerm){
							reply.ErrorIndex = i+1
							break
						}
					}

				}else{
					reply.ErrorIndex = len(rf.log)-1;
				}

			}
		} else {
			reply.Success = false
			reply.Term = currentTerm

		}

	}
}

func (rf *Raft) commitEntries(newCommit int){
	//Locked by AppendEntries
	lastCommit := rf.commitIndex
	if lastCommit > newCommit {
		return
	}

	for i:=lastCommit+1;i<=newCommit;i++{
		rf.ApplyMsgChn <- ApplyMsg{CommandValid: true, Command: rf.log[i].Command, CommandIndex: i}
	}
	rf.commitIndex = newCommit

}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, votesReceived *int32, requestsSent *int32, WrongLog *bool) bool {
	//SAFE
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	atomic.AddInt32(requestsSent, 1)
	if !ok {
		return ok
	}

	if reply.VoteGranted {
		print(rf.me, " received vote from ", server, "\n")
		atomic.AddInt32(votesReceived, 1)
	} else {
		//wait for all the requests to be sent and then set rf.currentTerm
		if(reply.WrongLog){
			*WrongLog = true
		}
		rf.changeTerm(reply.Term)
		rf.persist()
		
	}

	return ok

}
func (rf *Raft) electionVoteHelper() int {
	var votesReceived int32
	var requestSent int32
	votesReceived = 0
	requestSent = 0
	WrongLog := false
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		var args RequestVoteArgs
		var reply RequestVoteReply
	
		args.Term = rf.currentTerm
		args.CandidateId = rf.me
		args.LastLogIndex = len(rf.log)
		args.LastLogTerm = rf.log[len(rf.log)-1].Term
		if(rf.votedFor != rf.me){
			return 0;
		}
		go rf.sendRequestVote(i, &args, &reply, &votesReceived, &requestSent, &WrongLog) // Has rf.mu.Lock
	}
	for int(atomic.LoadInt32(&requestSent)) != len(rf.peers)-1  {
		
	}
	if(WrongLog){
		rf.stopElectionTimeOut()
	}
	return int(votesReceived)
}

func (rf *Raft) startElection() {

	rf.stopElectionTimeOut()
	rf.mu.Lock()
	rf.changeState("candidate")
	rf.changeTerm(rf.currentTerm + 1)
	rf.votedFor = rf.me
	rf.mu.Unlock()
	rf.startElectionTimeOut()
	votes := rf.electionVoteHelper() //has rf.mu.Lock
	print(rf.me, " received ", votes, " votes\n")


	if votes+ 1 > len(rf.peers)/2 && rf.votedFor == rf.me {
		rf.changeState("leader")
	} else {
		// time.Sleep(100 * time.Millisecond)
	}


}



func (rf *Raft) initializeLogListener() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		// print(len(rf.log))
		go rf.LogListener(i)
	}
}


func (rf *Raft) LogListener(server int) {
	for !rf.killed() && rf.State == "leader" {
		rf.mu.Lock()
		if rf.State != "leader" {
			rf.mu.Unlock()
			return;
		}
		rf.mu.Unlock()
		lengthOfLog := len(rf.log)
		nextIndexValue := rf.nextIndex[server]

		if nextIndexValue > lengthOfLog|| lengthOfLog <= nextIndexValue{
			time.Sleep(80 * time.Millisecond)
			continue
		}

		rf.sendAppendEntries(server)

		time.Sleep(80 * time.Millisecond)
	}

}
func (rf *Raft) commitIndexListener() {

	for rf.killed() == false && rf.State == "leader" {
		rf.matchIndex[rf.me] = len(rf.log) - 1
		matchIndexCopy := make([]int, len(rf.matchIndex))
		copy(matchIndexCopy, rf.matchIndex)
		sort.Ints(matchIndexCopy)

		if(rf.log[matchIndexCopy[len(rf.matchIndex)/2]].Term == rf.currentTerm){
			newCommit := max(rf.commitIndex, matchIndexCopy[len(rf.matchIndex)/2])
			rf.sendCommitEntries2(rf.commitIndex, newCommit)
			rf.commitIndex = newCommit
		}
			time.Sleep(30 * time.Millisecond)
	}
	
}

func (rf *Raft) sendCommitEntries2(lastCommit int, newCommit int) {
	if lastCommit > newCommit {
		return
	}

	for i := lastCommit+1; i <= newCommit; i++ {
		rf.ApplyMsgChn <- ApplyMsg{CommandValid: true, Command: rf.log[i].Command, CommandIndex: i}
	}

}
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}
func (rf *Raft) ticker() {
	// rf.startElection()
	rf.changeState("follower")
	rf.ApplyMsgChn <- ApplyMsg{CommandValid: true, Command: -1000, CommandIndex: 0}
	
	for !rf.killed() {
		select {
		case <-rf.electionTimeOut.C:
			if rf.State == "leader" {
				rf.stopElectionTimeOut()
				continue
			}
			print("Election started ", rf.me, "\n")

			rf.startElection();


			print("Election over ", rf.me, "\n")	


		default:
			continue
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
// Make() must return quickly, so it should start goroutin100es
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogItem{{Term: 0, Command: -1000, LogItemIndex: 0}}
	rf.votesReceived = 0
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = []int{}
	rf.matchIndex = []int{}
	rf.ApplyMsgChn = applyCh

	rf.setRandomTime();
	for i := 0; i < len(peers); i++ {
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}

	// rf.ApplyMsgChn <- ApplyMsg{CommandValid: true, Command: nil, CommandIndex: 0}
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
