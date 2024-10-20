package raft

import (
	"fmt"
)

type LogEntry struct {
	Term    int
	Command interface{}
}

func toString(entries []LogEntry) string {
	s := ""
	keys := []int{}
	entryMap := make(map[int]int)
	for _, entry := range entries {
		term := entry.Term
		if entryMap[term] == 0 {
			keys = append(keys, term)
		}
		entryMap[term]++
	}
	for _, value := range keys {
		s += fmt.Sprintf(" %d * %d ", value, entryMap[value])
	}
	return s
}

func (rf *Raft) ToLogIndex(index int) int {
	if index < rf.logStart {
		return -1
	}
	return index - rf.logStart
}

func (rf *Raft) rangeCheck(index int) {
	size := rf.size()
	if index < 0 || index > size {
		Fatalf("S%d : IndexOutOfBounds<Index: %d, Size: %d>", rf.me, index, size)
	}
}

func (rf *Raft) get(index int) LogEntry {
	resultEntry := LogEntry{rf.snapShot.SnapshotTerm, nil}
	rf.rangeCheck(index)
	logIndex := rf.ToLogIndex(index)
	if logIndex != -1 {
		resultEntry = rf.log[logIndex]
	}
	return resultEntry
}

// Returns the index of the first occurrence of the specified term in log entries,
// or 0 if log does not contain this term.
func (rf *Raft) indexOf(term int) int {
	for i, entry := range rf.log {
		if entry.Term == term {
			return i + rf.logStart
		}
	}
	return 0
}

// Returns the index of the last occurrence of the specified term in log entries,
// or 0 if log does not contain this term.
func (rf *Raft) lastIndexOf(term int) int {
	end := len(rf.log) - 1
	for i := end; i >= 0; i-- {
		if rf.log[i].Term == term {
			return i + rf.logStart
		}
	}
	return 0
}

func (rf *Raft) size() int {
	return len(rf.log) + rf.logStart - 1
}
