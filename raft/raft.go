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
	"cs651/labgob"
	"cs651/labrpc"
	"sort"
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
	mu                 sync.Mutex // Lock to protect shared access to this peer's state
	termLocker         sync.Mutex
	peers              []*labrpc.ClientEnd // RPC end points of all peers
	persister          *Persister          // Object to hold this peer's persisted state
	me                 int                 // this peer's index into peers[]
	dead               int32               // set by Kill()
	randomMilliseconds int
	ApplyMsgChn        chan ApplyMsg
	currentTerm        int
	votedFor           int
	log                []LogItem

	votesReceived int32
	commitIndex   int
	lastApplied   int

	nextIndex  []int
	matchIndex []int

	State string	


	electionTimeOut   *time.Ticker
	snapshot          []byte
	LastSnapshotindex int
	LastSnapshotTerm  int

	optCh chan ApplyMsg
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
	// e.Encode(rf.LastSnapshotindex)
	// e.Encode(rf.LastSnapshotTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.LastSnapshotTerm)
	e.Encode(rf.LastSnapshotindex)
	data := w.Bytes()

	rf.persister.SaveStateAndSnapshot(data, rf.snapshot)

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	// d.Decode(&rf.LastSnapshotindex)
	// d.Decode(&rf.LastSnapshotTerm)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.log)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.LastSnapshotTerm)
	d.Decode(&rf.LastSnapshotindex)
	rf.commitIndex = rf.LastSnapshotindex
}

// the service says it has created a snapshot that has
// all info up to and including inde,x. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {

	go rf.helperSnapshot(index, snapshot)

}
func (rf *Raft) helperSnapshot(index int, snapshot []byte) {
	// print("Snapshotting Started", rf.me, " ", index, "\n")


	rf.mu.Lock()

	if index <= rf.LastSnapshotindex {
		rf.mu.Unlock()
		return
	}


	rf.snapshot = snapshot
	rf.LastSnapshotTerm = rf.log[index-rf.LastSnapshotindex-1].Term

	rf.log = rf.log[index-rf.LastSnapshotindex:]
	rf.LastSnapshotindex = index



	// print("--------------a8******Snapshotting Started", rf.me, " ", index, " ", rf.LastSnapshotindex, " ", rf.State, "\n")
	rf.persist()
	rf.mu.Unlock()
	// for i :=0; i < len(rf.peers); i++ {
	// 	if i == rf.me {
	// 		continue
	// 	}
	// 	go rf.sendSnapshot(i)
	// }

	// print("Snapshotting done", rf.me, " ", index, "\n")
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
		rf.nextIndex[i] =  rf.LastSnapshotindex + 1
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
		myTerm := 0
		if len(rf.log) == 0 {
			myTerm = rf.LastSnapshotTerm
		} else {
			myTerm = rf.log[len(rf.log)-1].Term
		}
		if (rf.votedFor == -1 || (rf.votedFor == args.CandidateId)) && rf.isMoreUpdateToDate(args.LastLogTerm, args.LastLogIndex, myTerm, rf.LastSnapshotindex+len(rf.log)) {
			// print(rf.me, " received  request vote request from ", args.CandidateId, " ", "My Term ", rf.currentTerm, " Voter Term", args.Term, " votedFor", rf.votedFor, "\n")
			reply.Term = rf.currentTerm
			reply.VoteGranted = true

			rf.votedFor = args.CandidateId
		} else {
			// print(rf.me, " (voted no) received  request vote request from ", args.CandidateId, " ", "My Term ", rf.currentTerm, " Voter Term", args.Term, " votedFor", rf.votedFor, "\n")

			reply.Term = rf.currentTerm
			reply.WrongLog = !rf.isMoreUpdateToDate(args.LastLogTerm, args.LastLogIndex, myTerm, rf.LastSnapshotindex+len(rf.log))
			reply.VoteGranted = false

		}
	}

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	currentTerm := rf.currentTerm
	if rf.State == "leader" {
		if args.Term >= currentTerm { //TODO CHANGE THIS
			// // print("Leader Stepping down ", rf.me, " with new term", rf.currentTerm, "\n")
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
			lengthOfLog := rf.LastSnapshotindex + len(rf.log) + 1

			if args.PrevLogIndex < rf.LastSnapshotindex {
				restLength := rf.LastSnapshotindex - args.PrevLogIndex
				args.PrevLogIndex = rf.LastSnapshotindex
				args.PrevLogTerm = rf.LastSnapshotTerm

				args.Entries = args.Entries[min(len(args.Entries), restLength):]

				println()
				// Log: 0 1 2 3 4 5
				// SnapshotIndex: 2
				// PrevLogIndex: 0
				// Entries: 1 2 3 4 5

			}

			properLogCondition := (args.PrevLogIndex < lengthOfLog) && ((args.PrevLogIndex > rf.LastSnapshotindex &&
				 args.PrevLogTerm == rf.log[args.PrevLogIndex-rf.LastSnapshotindex-1].Term) ||
				(args.PrevLogIndex == rf.LastSnapshotindex && args.PrevLogTerm == rf.LastSnapshotTerm))
			if properLogCondition {
				reply.Success = true
				reply.Term = currentTerm
				reply.IsError = false
				rf.log = rf.log[:args.PrevLogIndex-rf.LastSnapshotindex]
				rf.log = append(rf.log, args.Entries...)
				// // print("Received ", rf.me, " ", args.LeaderId, " ", args.PrevLogIndex, " ", len(args.Entries), " ", len(rf.log), " leaderCommit:", args.LeaderCommit, " commitIndex: ", rf.commitIndex, "\n")
				rf.commitEntries(min(args.LeaderCommit, len(rf.log)+rf.LastSnapshotindex))

			} else {
				reply.Success = false
				reply.Term = rf.currentTerm
				reply.IsError = true
				reply.ErrorLength = len(rf.log)
				if args.PrevLogIndex < len(rf.log) {
					reply.ErrorTerm = rf.log[args.PrevLogIndex].Term
					for i := args.PrevLogIndex; i >= 0; i-- {
						if rf.log[i].Term != reply.ErrorTerm {
							reply.ErrorIndex = i + 1
							break
						}
					}

				} else {
					reply.ErrorIndex = len(rf.log) - 1
				}

			}
		} else {
			reply.Success = false
			reply.Term = currentTerm

		}

	}
}

func (rf *Raft) sendAppendEntries(server int) bool {
	// }
	// // print("")
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
	lengthOfLog := rf.LastSnapshotindex + 1 + len(rf.log)

	appendEntriesArgs.PrevLogIndex = min(nextIndexValue-1, lengthOfLog-1)

	// if( appendEntriesArgs.PrevLogIndex < rf.log[0].LogItemIndex){
	// 	return false;
	// }

	var PrevLogTerm int
	if appendEntriesArgs.PrevLogIndex == rf.LastSnapshotindex {
		PrevLogTerm = rf.LastSnapshotTerm
	} else if appendEntriesArgs.PrevLogIndex > rf.LastSnapshotindex {
		PrevLogTerm = rf.log[appendEntriesArgs.PrevLogIndex-rf.LastSnapshotindex-1].Term

	} else {
		//Send the fucking snapshot instead of AppendEntries
		rf.mu.Unlock()
		return rf.sendSnapshot(server)
	}

	entries := rf.log[max(0, appendEntriesArgs.PrevLogIndex-rf.LastSnapshotindex):]
	rf.mu.Unlock()

	// // print("sending ", rf.me, " ", server, " ", len(entries), "\n")
	appendEntriesArgs.PrevLogTerm = PrevLogTerm
	appendEntriesArgs.Entries = entries
	appendEntriesArgs.LeaderCommit = rf.commitIndex

	ok := rf.peers[server].Call("Raft.AppendEntries", &appendEntriesArgs, &appendEntriesReply)

	if !ok {
		return ok
	}


	if appendEntriesReply.Success {

		if len(entries) > 0 {
			rf.nextIndex[server] = nextIndexValue + len(entries)
			rf.matchIndex[server] = rf.nextIndex[server] - 1

		}
	} else {
		if appendEntriesReply.Term > rf.currentTerm {
			// print("Leader Stepping down ", rf.me, " with new term", rf.currentTerm, "\n")
			rf.mu.Lock()
			rf.changeState("follower")
			rf.changeTerm(appendEntriesReply.Term)
			rf.persist()
			rf.mu.Unlock()

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

	return ok
}

func (rf *Raft) sendSnapshot(server int) bool {
	// println("-------------------------Locking_________________sendSnapshot ", rf.me, server)
	rf.mu.Lock()
	// println("-------------------------GotLocking_________________sendSnapshot ", rf.me, server)

	args := InstallSnapshotArgs{}
	reply := InstallSnapshotReply{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LastIncludedIndex = rf.LastSnapshotindex
	args.LastIncludedTerm = rf.LastSnapshotTerm
	args.Data = rf.snapshot
	rf.mu.Unlock()	
	ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)

	if !ok {
		return ok
	} else {
		if reply.Success {
			rf.nextIndex[server] = rf.LastSnapshotindex + 1
			rf.matchIndex[server] = rf.LastSnapshotindex
		} else {
			if reply.Term > rf.currentTerm {
				rf.mu.Lock()
				rf.changeTerm(reply.Term)
				rf.changeState("follower")
				rf.persist()
				rf.mu.Unlock()
			}
		}
	}

	return true
}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, votesReceived *int32, requestsSent *int32, WrongLog *bool) bool {
	//SAFE
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	atomic.AddInt32(requestsSent, 1)
	if !ok {
		return ok
	}

	if reply.VoteGranted {
		// print(rf.me, " received vote from ", server, "\n")
		atomic.AddInt32(votesReceived, 1)
	} else {
		//wait for all the requests to be sent and then set rf.currentTerm
		if reply.WrongLog {
			*WrongLog = true
		}
		rf.changeTerm(reply.Term)
		rf.persist()

	}

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

		rf.log = append(rf.log, LogItem{Term: rf.currentTerm, Command: command, LogItemIndex: rf.LastSnapshotindex + 1 + len(rf.log)})
		// // print(len(rf.log))
		index = rf.log[len(rf.log)-1].LogItemIndex
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
	// print(rf.me, " is killed ", rf.State, " term ", rf.currentTerm, "\n")
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

func (rf *Raft) commitEntries(newCommit int) {
	//Locked by AppendEntries

	lastCommit := rf.commitIndex - rf.LastSnapshotindex - 1

	if lastCommit > newCommit {
		return
	}
	// // print("wowowo " ,newCommit, " ", lastCommit, "\n")
	for i := lastCommit + 1; i <= newCommit-rf.LastSnapshotindex-1; i++ {

		rf.ApplyMsgChn <- ApplyMsg{CommandValid: true, Command: rf.log[i].Command, CommandIndex: rf.log[i].LogItemIndex}
	
		rf.commitIndex++;
	}
	// rf.commitIndex = newCommit
	// logcopy := make([]LogItem, len(rf.log))
	// copy(logcopy, rf.log)
	// rf.helper(lastCommit, newCommit-rf.LastSnapshotindex-1, logcopy)
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
		args.LastLogIndex = rf.LastSnapshotindex + len(rf.log)
		if len(rf.log) == 0 {
			args.LastLogTerm = rf.LastSnapshotTerm
		} else {
			args.LastLogTerm = rf.log[len(rf.log)-1].Term
		}

		if rf.votedFor != rf.me {
			return 0
		}
		go rf.sendRequestVote(i, &args, &reply, &votesReceived, &requestSent, &WrongLog) // Has rf.mu.Lock
	}
	for int(atomic.LoadInt32(&requestSent)) != len(rf.peers)-1 {

	}
	if WrongLog {
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
	// print(rf.me, " received ", votes, " votes\n")

	if votes+1 > len(rf.peers)/2 && rf.votedFor == rf.me {
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
			return
		}
		rf.mu.Unlock()
		lengthOfLog := len(rf.log) + rf.LastSnapshotindex + 1
		nextIndexValue := rf.nextIndex[server]

		if nextIndexValue > lengthOfLog || lengthOfLog <= nextIndexValue {
			time.Sleep(20 * time.Millisecond)
			continue
		}

		rf.sendAppendEntries(server)


	}

}
func (rf *Raft) commitIndexListener() {

	for !rf.killed() && rf.State == "leader" {

		rf.mu.Lock()

		rf.matchIndex[rf.me] = rf.LastSnapshotindex + len(rf.log)
		matchIndexCopy := make([]int, len(rf.matchIndex))
		copy(matchIndexCopy, rf.matchIndex)
		sort.Ints(matchIndexCopy)

		median := matchIndexCopy[len(rf.matchIndex)/2]
		if median <= rf.LastSnapshotindex {
			rf.mu.Unlock()
			continue
		}

		if rf.log[median-rf.LastSnapshotindex-1].Term == rf.currentTerm {
			newCommit := max(rf.commitIndex, matchIndexCopy[len(rf.matchIndex)/2])

			rf.sendCommitEntries2(rf.commitIndex, newCommit)

		}

		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}

}

func (rf *Raft) sendCommitEntries2(lastCommit int, newCommit int) {
	lastCommit = rf.commitIndex - rf.LastSnapshotindex - 1

	lastCommit = max(lastCommit, -1)
	if lastCommit > newCommit {
		return
	}

	for i := lastCommit + 1; i <= newCommit-rf.LastSnapshotindex-1; i++ {
		// print("----------------------Committing ", rf.me, " ",  rf.log[i].LogItemIndex, " ", "\n")
		rf.ApplyMsgChn <- ApplyMsg{CommandValid: true, Command: rf.log[i].Command, CommandIndex: rf.log[i].LogItemIndex}
	
		rf.commitIndex++;
	}
	// print("Committing ", rf.me, " ", lastCommit, " ", newCommit - rf.LastSnapshotindex - 1, "\n")
	// rf.commitIndex = newCommit
	// logcopy := make([]LogItem, len(rf.log))
	// copy(logcopy, rf.log)
	// rf.helper(lastCommit, newCommit-rf.LastSnapshotindex-1, logcopy)


}
func (rf *Raft) helper(lastCommit int, newCommit int, logcopy []LogItem) {
	for i := lastCommit + 1; i <= newCommit; i++ {
		// print("Committing ", rf.me, " ", rf.log[i].LogItemIndex, " ", rf.log[i].Command.(int), "\n")
		rf.ApplyMsgChn <- ApplyMsg{CommandValid: true, Command: logcopy[i].Command, CommandIndex: logcopy[i].LogItemIndex}
	
	}
}
func (rf *Raft) InstallSnapshot(snapshotArgs *InstallSnapshotArgs, snapshotReply *InstallSnapshotReply) {

	// Your code here (2D).
	// println("-------------------------Locking_________________INstallSnapshot ", rf.me)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	// println("-------------------------AQLocking_________________INstallSnapshot ", rf.me)

	if snapshotArgs.Term < rf.currentTerm {
		snapshotReply.Term = rf.currentTerm
		snapshotReply.Success = false
	} else {
		if rf.State == "leader" {
			snapshotReply.Term = rf.currentTerm
			snapshotReply.Success = false
		} else if rf.State == "candidate" {
			snapshotReply.Term = rf.currentTerm
			snapshotReply.Success = false

			rf.changeTerm(snapshotArgs.Term)
			rf.resetElectionTimeOut()
			rf.changeState("follower")
		} else {
			// print("Snapshotting Success", rf.me, " ", snapshotArgs.LastIncludedIndex, "\n")
			snapshotReply.Success = true
			snapshotReply.Term = rf.currentTerm
			rf.resetElectionTimeOut()

			rf.changeTerm(snapshotArgs.Term)
			if snapshotArgs.LastIncludedIndex > rf.commitIndex {
				index := snapshotArgs.LastIncludedIndex
				if index >= len(rf.log)-1 {
					rf.log = rf.log[:0]
				} else {
					rf.log = rf.log[index-rf.LastSnapshotindex :]
				}

				rf.LastSnapshotindex = snapshotArgs.LastIncludedIndex
				rf.LastSnapshotTerm = snapshotArgs.LastIncludedTerm

				rf.snapshot = snapshotArgs.Data
				rf.commitIndex = max(rf.commitIndex, rf.LastSnapshotindex)
				rf.ApplyMsgChn <- ApplyMsg{SnapshotValid: true, Snapshot: snapshotArgs.Data, SnapshotTerm: snapshotArgs.LastIncludedTerm, SnapshotIndex: snapshotArgs.LastIncludedIndex}

			}
		}
	}

}
func (rf *Raft) ticker() {
	// rf.startElection()
	rf.changeState("follower")
	if rf.commitIndex < 0 {

		rf.ApplyMsgChn <- ApplyMsg{CommandValid: true, CommandIndex: 0, Command: nil}
	}
	for !rf.killed() {
		select {
		case <-rf.electionTimeOut.C:
			if rf.State == "leader" {
				rf.stopElectionTimeOut()
				continue
			}
			// print("Election started ", rf.me, "\n")

			rf.startElection()

			// print("Election over ", rf.me, "\n")

		default:
			continue
		}

	}

}

func (rf *Raft) channelListener() {
	// for !rf.killed() {
	// 	rf.ApplyMsgChn <- <-rf.optCh
	// 	// print("sendingggggg")
	// }
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
	rf.log = []LogItem{{Term: 0, LogItemIndex: 0}}
	rf.votesReceived = 0
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = []int{}
	rf.matchIndex = []int{}
	rf.ApplyMsgChn = applyCh
	go rf.channelListener()
	rf.LastSnapshotindex = -1
	rf.LastSnapshotTerm = -1
	rf.snapshot = nil
	rf.optCh = make(chan ApplyMsg, 20000)
	rf.setRandomTime()
	for i := 0; i < len(peers); i++ {
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}


	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()
	if rf.LastSnapshotindex != -1{
		rf.ApplyMsgChn <- ApplyMsg{CommandValid: false, SnapshotValid: true, Snapshot: rf.snapshot, SnapshotTerm: rf.LastSnapshotTerm, SnapshotIndex: rf.LastSnapshotindex}
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
