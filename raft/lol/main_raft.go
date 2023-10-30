package raft2
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
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

import "bytes"
import "cs651/labgob"

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

type LogItem struct {
	Term         int
	Command      interface{}
	LogItemIndex int
}

// A Go object implementing a single Raft peer.
type Raft2 struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	requestVoteLocker sync.Mutex
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

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

func (rf *Raft) startElectionTimeOut() {

	randomMilliseconds := rand.Intn(300-200+1) + 200
	rf.electionTimeOut = time.NewTicker(time.Duration(randomMilliseconds) * time.Millisecond)
	rf.resetElectionTimeOut();

}
func (rf *Raft) waitForElection() {

	randomMilliseconds := rand.Intn(300-200+1) + 200
	time.Sleep(time.Duration(randomMilliseconds) * time.Millisecond)
}

func (rf *Raft) resetElectionTimeOut() {

	randomMilliseconds := rand.Intn(300-200+1) + 200
	rf.electionTimeOut.Reset(time.Duration(randomMilliseconds) * time.Millisecond)
}

func (rf *Raft) initializeNextIndex(){
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] =  1
		rf.matchIndex[i] = 0
	}
}
func (rf *Raft) changeState(to string) {

	atomic.StoreInt32(&rf.votesReceived, 0)
	if to == rf.State {
		return
	}
	if to == "leader" {
		print("Leader ", rf.me, "\n")
		rf.initializeNextIndex()
		rf.votedFor = -1
		rf.stopElectionTimeOut()
		rf.State = "leader"
		go rf.leaderListeners()
		go rf.sendHeartBeat()

	} else if to == "candidate" {

		rf.State = "candidate"

	} else {

		rf.votedFor = -1
		rf.State = "follower"

		rf.stopElectionTimeOut()

		rf.startElectionTimeOut()
	}
}



// return currentTerm and whether this server
// believes it is the leader.time.NewTicker
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool

	term = rf.currentTerm
	isleader = (rf.State == "leader")
	// print(
	// 	"Requested Leader", isleader, " ", rf.State, " ", rf.me, "\n",
	// )
	// Your code here (2A).
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.SaveStateAndSnapshot() or use persister.SaveRaftState().
// after you've implemented snapshots, pass the current snapshot to persister.SaveStateAndSnapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)
	e.Encode(rf.votedFor)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.log)
	d.Decode(&rf.votedFor)
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

func (rf *Raft) isMoreUpdateToDate(candidateTerm int, candidateIndex int, myTerm int, myIndex int) bool {
	if candidateTerm > myTerm {
		return true
	} else if candidateTerm == myTerm {
		if candidateIndex >= myIndex {
			return true
		}
	}
	return false
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.persist()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm || rf.State == "leader" {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		// return
	}else{

		
		if (rf.votedFor == -1 || (rf.votedFor == args.CandidateId)) && rf.currentTerm < args.Term && rf.isMoreUpdateToDate(args.LastLogTerm, args.LastLogIndex, rf.log[len(rf.log)-1].Term, len(rf.log)) {
			print(rf.me, " received  request vote request from ", args.CandidateId, " ", "My Term ", rf.currentTerm, " Voter Term", args.Term, " votedFor", rf.votedFor, "\n")
			rf.votedFor = args.CandidateId
		
			reply.Term = rf.currentTerm
			rf.currentTerm = args.Term
			reply.VoteGranted = true
			return
		} else {
			rf.currentTerm = max(rf.currentTerm,args.Term) // TODO CHANGE THIS
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			return

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok == false {
		return ok
	}
	rf.mu.Lock()

	if rf.State == "candidate" {
		if reply.VoteGranted == true {
			rf.votesReceived += 1
			if int(rf.votesReceived)+1 > len(rf.peers)/2 {
				print("Becomes Leader ", rf.me, "with votes: ", rf.votesReceived, "\n")
				rf.changeState("leader")

			}
		} else {
			rf.currentTerm = max(rf.currentTerm,reply.Term)
		}

	}

	rf.persist()
	rf.mu.Unlock()
	return ok
}

func (rf *Raft) sendAppendEntries(server int) bool {


	rf.mu.Lock()

	appendEntriesArgs := AppendEntriesArgs{}
	appendEntriesReply := AppendEntriesReply{}
	appendEntriesArgs.Term = rf.currentTerm
	appendEntriesArgs.LeaderId = rf.me

	nextIndexValue := rf.nextIndex[server] 
	lengthOfLog := len(rf.log)
	appendEntriesArgs.PrevLogIndex = min(nextIndexValue - 1, lengthOfLog - 1)
	PrevLogTerm := rf.log[appendEntriesArgs.PrevLogIndex].Term
	entries := rf.log[nextIndexValue:]
	
	appendEntriesArgs.PrevLogTerm = PrevLogTerm
	appendEntriesArgs.Entries = entries
	appendEntriesArgs.LeaderCommit = rf.commitIndex
	ok := rf.peers[server].Call("Raft.AppendEntries", &appendEntriesArgs, &appendEntriesReply)

	if !ok {
		rf.mu.Unlock()
		return false
	}



	
	if appendEntriesReply.Success == true {
		if len(entries) > 0 {
			rf.nextIndex[server] = nextIndexValue + len(entries)
			rf.matchIndex[server] = rf.nextIndex[server] - 1

		}
		// print("sendAppendEntries ", rf.me, " ", server, " ", len(rf.log), " ", rf.nextIndex[server], "\n")
	} else {
		if appendEntriesReply.Term > rf.currentTerm {
			print("Leader Stepping down ", rf.me, " with new term", rf.currentTerm, "\n")
			rf.currentTerm = appendEntriesReply.Term
			rf.changeState("follower")
		}
		if(appendEntriesReply.IsError == true){
			// rf.nextIndex[server] -= 1
			if(appendEntriesReply.ErrorLength <= appendEntriesArgs.PrevLogIndex){
				rf.nextIndex[server] = appendEntriesReply.ErrorLength
			}else{
				rf.nextIndex[server] = appendEntriesReply.ErrorIndex
				for i := 0; i < len(rf.log); i++ {
					if rf.log[i].Term == appendEntriesReply.ErrorLength{
						rf.nextIndex[server] = i
						break
					}
				}
			}
			// rf.nextIndex[server] = 1;
			print("PrevLogIndexError Server: ", rf.me, " ", server, " ", rf.nextIndex[server], " ", appendEntriesReply.ErrorLength, "\n")
		}

	}

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

func (rf *Raft) leaderListeners() {
	go rf.commitIndexListener()
	go rf.initializeLogListener()
}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}
func (rf *Raft) commitIndexListener() {

	for rf.killed() == false && rf.State == "leader" {

		// print("\nCommit Index Listener ", rf.me, " ", rf.commitIndex, "\n")
		rf.matchIndex[rf.me] = len(rf.log) - 1

		matchIndexCopy := make([]int, len(rf.matchIndex))
		copy(matchIndexCopy, rf.matchIndex)
		sort.Ints(matchIndexCopy)

		if(rf.log[matchIndexCopy[len(rf.matchIndex)/2]].Term == rf.currentTerm){
			rf.commitIndex = max(rf.commitIndex, matchIndexCopy[len(rf.matchIndex)/2])
			rf.sendCommitEntries2(rf.lastApplied, rf.commitIndex)
			rf.lastApplied = rf.commitIndex
		}


		// print("Commit Index ", rf.me, " ", rf.commitIndex, "\n")
		// print(matchIndexCopy[len(rf.matchIndex)/2], "\n")
		time.Sleep(30 * time.Millisecond)

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

func (rf *Raft) printLog() {
	print("Log ", rf.me, " ", rf.currentTerm, " ", rf.State, " ", rf.votedFor, " ", len(rf.log), " ", rf.commitIndex, " ", rf.lastApplied, "\n")
	for i := 1; i < len(rf.log); i++ {
		print(rf.log[i].Command.(int), " ")
	}
	print("\n")
}
func (rf *Raft) LogListener(server int) {
	for rf.killed() == false && rf.State == "leader" {
		// print("New log ", len(rf.log)," " , rf.nextIndex[server], "\n")
		break
		lengthOfLog := len(rf.log)
		nextIndexValue := rf.nextIndex[server]

		if nextIndexValue > lengthOfLog|| lengthOfLog <= nextIndexValue{
			time.Sleep(80 * time.Millisecond)
			continue
		}
		// print("yoyoyo")

		// print("Log Listener ", rf.me, " ", server, " ", len(rf.log), " ", rf.nextIndex[server], "\n")
		
		rf.sendAppendEntries(server)
		time.Sleep(80 * time.Millisecond)
	}

}
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := (rf.State == "leader")
	if !isLeader {
		return index, term, isLeader
	} else {


		rf.mu.Lock()

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

func (rf *Raft) sendCommitEntries(lastCommit int, newCommit int) {
	// print("sendCommitEntries ", rf.me, " ", lastCommit, " ", newCommit, "\n")
	if lastCommit > newCommit {
		return
	}

	// print("sendCommitEntries ", rf.me, " ", lastCommit, " ", newCommit, "\n")
	rf.commitIndex = newCommit
	// print("---log inside commmit for ", rf.me, "\n")
	// for i := 1; i < len(rf.log); i++ {
	// 	print(rf.log[i].Command.(int), " ")
	// }
	// print("\n")
	rf.printLog()
	for i := rf.lastApplied + 1; i <= newCommit; i++ {
		print("sendCommitEntries for ", rf.me, "  lastcommit: ", lastCommit, " newcommit:", newCommit, " index:", i, " length:", len(rf.log), " value:", rf.log[i].Command.(int), "\n")
		rf.ApplyMsgChn <- ApplyMsg{CommandValid: true, Command: rf.log[i].Command, CommandIndex: i}
	}
	rf.lastApplied = newCommit
}

func (rf *Raft) sendCommitEntries2(lastCommit int, newCommit int) {
	// print("sendCommitEntries ", rf.me, " ", lastCommit, " ", newCommit, "\n")
	if lastCommit > newCommit {
		return
	}

	// print("sendCommitEntries ", rf.me, " ", lastCommit, " ", newCommit, "\n")
	rf.commitIndex = newCommit
	// print("|||||||log inside commmit for ", rf.me, "\n")
	// for i := 1; i < len(rf.log); i++ {
	// 	print(rf.log[i].Command.(int), " ")
	// }
	// print("\n")
	rf.printLog()
	for i := rf.lastApplied + 1; i <= newCommit; i++ {
		print("|||||||||||||||||sendCommitEntries for ", rf.me, "  lastcommit: ", lastCommit, " newcommit:", newCommit, " index:", i, " length : ", len(rf.log),  " value:", rf.log[i].Command.(int), "\n")
		rf.ApplyMsgChn <- ApplyMsg{CommandValid: true, Command: rf.log[i].Command, CommandIndex: i}
	}
	rf.lastApplied = newCommit
}
func (rf *Raft) addToLog(args *AppendEntriesArgs) {
	if len(args.Entries) > 0 {
		rf.log = rf.log[:args.PrevLogIndex+1]
		rf.log = append(rf.log, args.Entries...)
	}
}

func min(x int, y int) int {
	if x < y {
		return x
	}
	return y
}


func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if len(args.Entries) > 0 {
		// print("hmmmm ", rf.me, " leaderID ", args.LeaderId, " leaderterm ", args.Term, " myterm ", rf.currentTerm, "\n")
	}

	defer rf.persist()
	defer rf.mu.Unlock()

	if rf.State == "leader" {
		if args.Term >= rf.currentTerm { //TODO CHANGE THIS
		
			print("Leader Stepping down ", rf.me, " with new term", rf.currentTerm, "\n")
		
			reply.Term = rf.currentTerm
			reply.Success = false
			reply.IsError = false
			rf.changeState("follower")
			rf.currentTerm = args.Term

		

		} else if args.Term < rf.currentTerm {
			reply.Success = false
			reply.Term = args.Term
		}

	} else if rf.State == "candidate" {

		if args.Term > rf.currentTerm { //TODO CHANGE THIS
			// THERE IS A LEADER
			rf.changeState("follower")
		
			reply.Term = rf.currentTerm
			reply.Success = false
			reply.IsError = false


			rf.currentTerm = args.Term
			// rf.addToLog(args)
			// rf.sendCommitEntries(rf.commitIndex, args.LeaderCommit)
		} else if args.Term < rf.currentTerm {
			reply.Success = false
			reply.Term = rf.currentTerm

		} else {

			rf.changeState("follower")
			reply.Success = false
			reply.Term = args.Term
			reply.IsError = false
			// rf.addToLog(args)
			// rf.sendCommitEntries(rf.commitIndex, args.LeaderCommit)

		}
	} else if rf.State == "follower" {

		if args.Term >= rf.currentTerm { //TODO CHANGE THIS
			

			rf.resetElectionTimeOut()

		
			if(args.PrevLogIndex < len(rf.log)&&args.PrevLogTerm == rf.log[args.PrevLogIndex].Term){

					reply.Success = true
					reply.Term = rf.currentTerm
					reply.IsError = false
					rf.currentTerm = args.Term
					rf.addToLog(args)
				    rf.sendCommitEntries(rf.commitIndex, min(min(args.PrevLogIndex,len(rf.log)-1),args.LeaderCommit))
				

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

			rf.currentTerm = args.Term
			

		} else {
			// print("FAILED APPEND ENTRIES\n")
			reply.Success = false
			reply.Term = rf.currentTerm

		}

	}

	



}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogItem
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	IsError bool
	ErrorTerm  int
	ErrorIndex int
	ErrorLength int
	
}

func (rf *Raft) startElection() {
	rf.stopElectionTimeOut()
	rf.changeState("candidate")
	rf.currentTerm += 1
	rf.votedFor = rf.me

	rf.votesReceived = 0;
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
		go rf.sendRequestVote(i, &args, &reply)
	}
	rf.persist()
	rf.waitForElection()
}

func (rf *Raft) sendHeartBeatHelper(server int){
	for rf.killed() == false && rf.State == "leader" {
		ok := rf.sendAppendEntries(server)
		if ok == true {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}
func (rf *Raft) sendHeartBeat() {
	for rf.killed() == false && rf.State == "leader" {
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}

			go rf.sendAppendEntries(i)
		}

		time.Sleep(100 * time.Millisecond)
	}
}
func (rf *Raft) ticker() {
	// rf.startElection()

	rf.changeState("follower")
	rf.ApplyMsgChn <- ApplyMsg{CommandValid: true, Command: -1000, CommandIndex: 0}
	for rf.killed() == false {

		if rf.electionTimeOut != nil {

			select {
			case <-rf.electionTimeOut.C:
				print("Election Time Out for ", rf.me, " with Term", rf.currentTerm+1, "\n")
				rf.startElection()
					rf.votedFor = -1
				
				rf.mu.Lock()
				if rf.State != "leader" {
							rf.votedFor = -1
					rf.resetElectionTimeOut()
				}
				rf.persist()
				rf.mu.Unlock()

			default:
				continue

			}
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
