package raft

import (

	"math/rand"


	"time"
	"log"
)


// Debugging
const Debug = false

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
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}


type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}
func init() {
	log.SetFlags(log.Lmicroseconds)
}
func (rf *Raft) startElectionTimeOut() {
	randomMilliseconds := rand.Intn(400-200+1) + 200
	rf.electionTimeOut = time.NewTicker(time.Duration(randomMilliseconds) * time.Millisecond)
	rf.resetElectionTimeOut();

}
func (rf *Raft) waitForElection() {
	randomMilliseconds := rand.Intn(150-50+1) + 50
	time.Sleep(time.Duration(randomMilliseconds) * time.Millisecond)
}
func (rf *Raft) stopElectionTimeOut() {
	if rf.electionTimeOut != nil {
		print("Timer Stopped for ", rf.me, "\n")
		rf.electionTimeOut.Stop()
	}

}
func (rf *Raft) resetElectionTimeOut() {
	randomMilliseconds := rand.Intn(400-200+1) + 200
	rf.electionTimeOut.Reset(time.Duration(randomMilliseconds) * time.Millisecond)
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
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
