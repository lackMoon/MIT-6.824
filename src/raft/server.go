package raft

func (rf *Raft) UpdateCommitIndexL(index int) {
	if index > rf.commitIndex {
		rf.commitIndex = index
		if rf.commitIndex > rf.lastApplied {
			//DPrintf("S%d : CommitLog, commitIndex = %d", rf.me, rf.commitIndex)
			rf.ToApply.Broadcast()
		}
	}
}

func (rf *Raft) UpdateTermL(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
	if rf.state != Follower {
		rf.ConvertStateL(Follower)
	}
}

func (rf *Raft) appendLogsL(startIndex int, entries []LogEntry) {
	logLength := len(rf.log)
	entriesLength := len(entries)
	conflictIndex := logLength
	logIdx := rf.ToLogIndex(startIndex)
	entriesIdx := 0
	for ; logIdx < logLength && entriesIdx < entriesLength; logIdx, entriesIdx = logIdx+1, entriesIdx+1 {
		if rf.log[logIdx] != entries[entriesIdx] {
			conflictIndex = logIdx
			break
		}
	}
	if entriesIdx < entriesLength {
		rf.log = append(rf.log[:conflictIndex], entries[entriesIdx:]...)
		rf.persist()
	}
}

func (rf *Raft) ConvertStateL(state RaftState) {
	currentState := rf.state
	rf.state = state
	switch state {
	case Follower:
		//DPrintf("S%d : Convert To Follower", rf.me)
		if currentState == Leader {
			rf.StopHeartBeatTimer()
			rf.ResetElectionTimer()
		}
	case Candidate:
		//DPrintf("S%d : Convert To Candidate", rf.me)
		rf.ResetElectionTimer()
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.persist()
		rf.requestVoteToPeersL()
	case Leader:
		DPrintf("S%d : Convert To Leader, Term = %d", rf.me, rf.currentTerm)
		rf.StopElectionTimer()
		lastLogIndex := rf.size()
		for i := range rf.nextIndex {
			rf.nextIndex[i] = lastLogIndex + 1
			rf.inFlightIndex[i] = 0
		}
		rf.ResetHeartBeatTimer()
		rf.sendRPCToPeers(true)
	}
}

func (rf *Raft) requestVoteToPeersL() {
	lastLogIndex := rf.size()
	lastLogTerm := rf.get(lastLogIndex).Term
	votes := 1
	args := &RequestVoteArgs{rf.currentTerm, rf.me, lastLogIndex, lastLogTerm}
	for i := range rf.peers {
		if i != rf.me {
			//DPrintf("RV Request: S%d -> S%d,Term = %d", rf.me, i, rf.currentTerm)
			go rf.sendRequestVote(i, args, &votes)
		}
	}
}

func (rf *Raft) sendRPCToPeers(isHeartBeat bool) {
	for i := range rf.peers {
		if i != rf.me {
			go func(peer int, isHeartBeat bool) {
				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}
				size := rf.size()
				if rf.persistIndex < size {
					rf.persistIndex = size
					rf.persist()
				}
				if rf.nextIndex[peer] < rf.logStart {
					if rf.snapShot.SnapshotIndex == rf.inFlightSnapShotIndex[peer] && !isHeartBeat {
						rf.mu.Unlock()
						return
					}
					args := &InstallSnapshotArgs{rf.currentTerm, rf.me, rf.snapShot.SnapshotIndex, rf.snapShot.SnapshotTerm, rf.snapShot.Snapshot}
					rf.inFlightSnapShotIndex[peer] = args.LastIncludedIndex
					rf.mu.Unlock()
					rf.sendInstallSnapShot(peer, args)
				} else {
					if size <= rf.inFlightIndex[peer] && !isHeartBeat {
						rf.mu.Unlock()
						return
					}
					prevLogIndex := rf.nextIndex[peer] - 1
					prevLogTerm := rf.get(prevLogIndex).Term
					entries := make([]LogEntry, size-prevLogIndex)
					copy(entries, rf.log[rf.ToLogIndex(prevLogIndex+1):])
					rf.inFlightIndex[peer] = prevLogIndex + len(entries)
					args := &AppendEntriesArgs{rf.currentTerm, rf.me, prevLogIndex, prevLogTerm, entries, rf.commitIndex}
					rf.mu.Unlock()
					rf.sendAppendEntries(peer, args)
				}
			}(i, isHeartBeat)
		}
	}
}

func (rf *Raft) commitLogL() {
	peerNums := len(rf.peers)
	size := rf.size()
	if rf.state != Leader {
		return
	}
	for i := size; i > rf.commitIndex; i-- {
		if rf.get(i).Term == rf.currentTerm {
			matches := 1
			for peer := 0; peer < peerNums; peer++ {
				if peer != rf.me && rf.matchIndex[peer] >= i {
					matches++
				}
			}
			if matches > peerNums/2 {
				rf.UpdateCommitIndexL(i)
				//DPrintf("S%d : CommitLog, currentTerm = %d,commitIndex = %d, commitTerm = %d", rf.me, rf.currentTerm, rf.commitIndex, rf.log[rf.commitIndex-rf.logStart].Term)
				break
			}
		}
	}
}
