package raft

func (rf *Raft) UpdateMatchIndexL(peer int, newMatch int) bool {
	if newMatch > rf.matchIndex[peer] {
		rf.matchIndex[peer] = newMatch
		return true
	}
	return false
}

func (rf *Raft) UpdateCommitIndexL(index int) {
	if index > rf.commitIndex {
		rf.commitIndex = index
		if rf.commitIndex > rf.lastApplied {
			rf.ToApply.Broadcast()
		}
	}
}

func (rf *Raft) UpdateTermL(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	if rf.state != Follower {
		rf.ConvertStateL(Follower)
	}
}

func (rf *Raft) appendLogsL(startIndex int, entries []LogEntry) {
	logLength := len(rf.log)
	entriesLength := len(entries)
	conflictIndex := logLength
	entriesIdx := 0
	for ; startIndex < logLength && entriesIdx < entriesLength; startIndex, entriesIdx = startIndex+1, entriesIdx+1 {
		if rf.log[startIndex] != entries[entriesIdx] {
			conflictIndex = startIndex
			break
		}
	}
	rf.log = append(rf.log[:conflictIndex], entries[entriesIdx:]...)
}

func (rf *Raft) ConvertStateL(state RaftState) {
	currentState := rf.state
	rf.state = state
	switch state {
	case Follower:
		DPrintf("S%d : Convert To Follower", rf.me)
		if currentState == Leader {
			rf.StopHeartBeatTimer()
			rf.ResetElectionTimer()
		}
	case Candidate:
		DPrintf("S%d : Convert To Candidate", rf.me)
		rf.ResetElectionTimer()
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.requestVoteToPeersL()
	case Leader:
		DPrintf("S%d : Convert To Leader, Term = %d", rf.me, rf.currentTerm)
		rf.StopElectionTimer()
		lastLogIndex := len(rf.log)
		for i := range rf.nextIndex {
			rf.nextIndex[i] = lastLogIndex + 1
			rf.inFlightIndex[i] = 0
		}
		rf.ResetHeartBeatTimer()
		rf.appendEntriesToPeersL(true)
	}
}

func (rf *Raft) requestVoteToPeersL() {
	lastLogIndex := len(rf.log)
	lastLogTerm := rf.get(lastLogIndex).Term
	votes := 1
	args := &RequestVoteArgs{rf.currentTerm, rf.me, lastLogIndex, lastLogTerm}
	for i := range rf.peers {
		if i != rf.me {
			DPrintf("RV Request: S%d -> S%d,Term = %d", rf.me, i, rf.currentTerm)
			go rf.sendRequestVote(i, args, &votes)
		}
	}
}

func (rf *Raft) appendEntriesToPeersL(isHeartBeat bool) {
	for i := range rf.peers {
		if i != rf.me {
			length := len(rf.log)
			if rf.state != Leader || (length <= rf.inFlightIndex[i] && !isHeartBeat) {
				continue
			}
			var entries []LogEntry
			prevLogIndex := rf.nextIndex[i] - 1
			prevLogTerm := rf.get(prevLogIndex).Term
			entries = make([]LogEntry, length-prevLogIndex)
			copy(entries, rf.log[prevLogIndex:])
			rf.inFlightIndex[i] = prevLogIndex + len(entries)
			args := &AppendEntriesArgs{rf.currentTerm, rf.me, prevLogIndex, prevLogTerm, entries, rf.commitIndex}
			DPrintf("AE Request: S%d -> S%d,isHeartBeat = %t,Term = %d,prevLogIndex = %d,prevLogTerm = %d,LeaderCommit = %d,entries = {%s}", args.LeaderId, i, isHeartBeat, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, toString(args.Entries))
			go rf.sendAppendEntries(i, args)
		}
	}
}

func (rf *Raft) commitLogL() {
	peerNums := len(rf.peers)
	length := len(rf.log)
	if rf.state != Leader {
		return
	}
	for i := length - 1; i >= rf.commitIndex; i-- {
		idx := i + 1
		if idx > rf.commitIndex && rf.log[i].Term == rf.currentTerm {
			matches := 1
			for peer := 0; peer < peerNums; peer++ {
				if peer != rf.me && rf.matchIndex[peer] >= idx {
					matches++
				}
			}
			if matches > peerNums/2 {
				rf.UpdateCommitIndexL(idx)
				//DPrintf("S%d : commitLog, commitIndex = %d", rf.me, rf.commitIndex)
				break
			}
		}
	}
}
