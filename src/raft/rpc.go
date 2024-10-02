package raft

import (
	"fmt"
)

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

type ConflictMsg struct {
	ConflictTerm  int
	ConflictIndex int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	Msg     ConflictMsg
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
			lastLogIndex := len(rf.log)
			lastLog := rf.get(lastLogIndex)
			if args.LastLogTerm > lastLog.Term || (args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLogIndex) {
				rf.votedFor = args.CandidateId
				rf.ResetElectionTimer()
				reply.VoteGranted = true
			}
		}
	}
	reply.Term = rf.currentTerm
	DPrintf("RV Response: S%d -> S%d,Granted = %t,Term = %d", rf.me, args.CandidateId, reply.VoteGranted, reply.Term)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) bool {
	reply := &AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		replyTerm := reply.Term
		if rf.currentTerm < replyTerm {
			rf.UpdateTermL(replyTerm)
		}
		if rf.state != Leader {
			return true
		}
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
			newMatch := args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = newMatch + 1
			if rf.UpdateMatchIndexL(server, newMatch) {
				rf.commitLogL()
			}
		}
	}
	return reply.Success
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	argTerm := args.Term
	length := len(rf.log)
	reply.Msg = ConflictMsg{-1, length}
	if rf.currentTerm <= argTerm {
		if rf.currentTerm < argTerm {
			rf.UpdateTermL(argTerm)
		}
		rf.ResetElectionTimer()
		prevLogIndex := args.PrevLogIndex
		if prevLogIndex <= length {
			term := rf.get(prevLogIndex).Term
			if term == args.PrevLogTerm {
				reply.Success = true
				rf.appendLogsL(prevLogIndex, args.Entries)
				if args.LeaderCommit > rf.commitIndex {
					rf.UpdateCommitIndexL(min(args.LeaderCommit, len(rf.log)))
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
