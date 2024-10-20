package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) StopElectionTimer() {
	if !rf.electionTimer.Stop() {
		select {
		case <-rf.electionTimer.C:
		default:
		}
	}
}

func (rf *Raft) ResetElectionTimer() {
	expiredTime := time.Duration(500+rand.Int63()%300) * time.Millisecond
	//DPrintf("S%d : ResetElectionTime", rf.me)
	rf.StopElectionTimer()
	rf.electionTimer.Reset(expiredTime)
}

func (rf *Raft) StopHeartBeatTimer() {
	if !rf.heartBeatTimer.Stop() {
		select {
		case <-rf.heartBeatTimer.C:
		default:
		}
	}
}

func (rf *Raft) ResetHeartBeatTimer() {
	//DPrintf("S%d : ResetHeartBeatTimer", rf.me)
	rf.StopHeartBeatTimer()
	rf.heartBeatTimer.Reset(RaftHeartBeatInterval)
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.ConvertStateL(Candidate)
			rf.mu.Unlock()
		case <-rf.heartBeatTimer.C:
			rf.mu.Lock()
			rf.ResetHeartBeatTimer()
			rf.sendRPCToPeers(true)
			rf.mu.Unlock()
		}
	}
}
