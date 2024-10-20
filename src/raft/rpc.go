package raft

import "fmt"

type ConflictMsg struct {
	ConflictTerm  int
	ConflictIndex int
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entrys
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
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
	Msg     ConflictMsg
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, votes *int) {
	reply := &RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		replyTerm := reply.Term
		if rf.currentTerm <= replyTerm {
			if rf.currentTerm < replyTerm {
				rf.UpdateTermL(replyTerm)
			}
			if rf.state == Candidate && reply.VoteGranted && rf.currentTerm == args.Term {
				*votes++
				if *votes > len(rf.peers)/2 {
					rf.ConvertStateL(Leader)
				}
			}
		}
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	candidateTerm := args.Term
	if rf.currentTerm <= candidateTerm {
		if rf.currentTerm < candidateTerm {
			rf.UpdateTermL(candidateTerm)
		}
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			lastLogIndex := rf.size()
			lastLog := rf.get(lastLogIndex)
			if args.LastLogTerm > lastLog.Term || (args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLogIndex) {
				rf.votedFor = args.CandidateId
				rf.persist()
				rf.ResetElectionTimer()
				reply.VoteGranted = true
			}
		}
	}
	reply.Term = rf.currentTerm
	//DPrintf("RV Response: S%d -> S%d,Granted = %t,Term = %d", rf.me, args.CandidateId, reply.VoteGranted, reply.Term)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	DPrintf("AE Request: S%d -> S%d, Term = %d,prevLogIndex = %d,prevLogTerm = %d,LeaderCommit = %d,entries = {%s}", args.LeaderId, server, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, toString(args.Entries))
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		replyTerm := reply.Term
		if rf.currentTerm <= replyTerm {
			if rf.currentTerm < replyTerm {
				rf.UpdateTermL(replyTerm)
			}
			if rf.state == Leader {
				if !reply.Success {
					xTerm := reply.Msg.ConflictTerm
					if xTerm != -1 {
						if idx := rf.lastIndexOf(xTerm); idx == 0 {
							rf.nextIndex[server] = reply.Msg.ConflictIndex
						} else {
							rf.nextIndex[server] = idx + 1
						}
					} else {
						rf.nextIndex[server] = reply.Msg.ConflictIndex + 1
					}
				} else {
					newNext := args.PrevLogIndex + len(args.Entries) + 1
					if newNext > rf.nextIndex[server] {
						rf.nextIndex[server] = newNext
						rf.matchIndex[server] = rf.nextIndex[server] - 1
						rf.commitLogL()
					}
				}
			}
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	leaderTerm := args.Term
	size := rf.size()
	reply.Msg = ConflictMsg{-1, size}
	if rf.currentTerm <= leaderTerm {
		if rf.currentTerm < leaderTerm {
			rf.UpdateTermL(leaderTerm)
		}
		rf.ResetElectionTimer()
		prevLogIndex := args.PrevLogIndex
		if prevLogIndex <= size {
			term := rf.get(prevLogIndex).Term
			if term == args.PrevLogTerm {
				reply.Success = true
				rf.appendLogsL(prevLogIndex+1, args.Entries)
				if args.LeaderCommit > rf.commitIndex {
					rf.UpdateCommitIndexL(min(args.LeaderCommit, size))
				}
			} else {
				reply.Msg.ConflictTerm = term
				reply.Msg.ConflictIndex = rf.indexOf(reply.Msg.ConflictTerm)
			}
		}
	}
	reply.Term = rf.currentTerm
	str := ""
	if reply.Success {
		str = fmt.Sprintf("CommitIndex = %d,Log = {%s}", rf.commitIndex, toString(rf.log))
	} else {
		str = fmt.Sprintf("ConflictTerm = %d,ConflictIndex = %d", reply.Msg.ConflictTerm, reply.Msg.ConflictIndex)
	}
	DPrintf("AE Response: S%d -> S%d,Term = %d,%s", rf.me, args.LeaderId, args.Term, str)
}

func (rf *Raft) sendInstallSnapShot(server int, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}
	DPrintf("InstallSnapShot Request: S%d -> S%d,Term = %d,lastIncludedIndex = %d,lastIncludedTerm = %d", args.LeaderId, server, args.Term, args.LastIncludedIndex, args.LastIncludedTerm)
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		replyTerm := reply.Term
		if rf.currentTerm <= replyTerm {
			if rf.currentTerm < reply.Term {
				rf.UpdateTermL(reply.Term)
			}
			newNext := args.LastIncludedIndex + 1
			if newNext > rf.nextIndex[server] {
				rf.nextIndex[server] = newNext
			}
		}
	}
}

func (rf *Raft) InstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	leaderTerm := args.Term
	if rf.currentTerm <= leaderTerm {
		if rf.currentTerm < leaderTerm {
			rf.UpdateTermL(leaderTerm)
		}
		rf.ResetElectionTimer()
		lastIncludedIndex := args.LastIncludedIndex
		if lastIncludedIndex > rf.snapShot.SnapshotIndex {
			size := rf.size()
			rf.snapShot = RaftSnapShot{args.Data, lastIncludedIndex, args.LastIncludedTerm}
			if lastIncludedIndex <= size && rf.get(lastIncludedIndex).Term == args.LastIncludedTerm {
				rf.log = append([]LogEntry{}, rf.log[rf.ToLogIndex(lastIncludedIndex)+1:]...)
			} else {
				rf.log = []LogEntry{}
			}
			rf.logStart = lastIncludedIndex + 1
			rf.persist()
			if rf.commitIndex < lastIncludedIndex {
				rf.commitIndex = lastIncludedIndex
			}
			if rf.lastApplied < lastIncludedIndex {
				rf.applySnapShot = true
				rf.ToApply.Broadcast()
			}
		}
	}
	reply.Term = rf.currentTerm
	DPrintf("InstallSnapShot Response: S%d -> S%d,Term = %d, log = {%s}", rf.me, args.LeaderId, reply.Term, toString(rf.log))
}
