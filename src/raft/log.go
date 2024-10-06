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

func (rf *Raft) rangeCheck(index int) {
	length := len(rf.log)
	if index < 0 || index > length {
		Fatalf("S%d : IndexOutOfBounds<Index: %d, Length: %d>", rf.me, index, length)
	}
}

func (rf *Raft) get(index int) LogEntry {
	resultEntry := LogEntry{0, nil}
	rf.rangeCheck(index)
	if index != 0 {
		resultEntry = rf.log[index-1]
	}
	return resultEntry
}

// Returns the index of the first occurrence of the specified term in log entries,
// or 0 if log does not contain this term.
func (rf *Raft) indexOf(term int) int {
	for i, entry := range rf.log {
		if entry.Term == term {
			return i + 1
		}
	}
	return 0
}

// Returns the index of the last occurrence of the specified term in log entries,
// or 0 if log does not contain this term.
func (rf *Raft) lastIndexOf(term int) int {
	length := len(rf.log)
	for i := length - 1; i >= 0; i-- {
		if rf.log[i].Term == term {
			return i + 1
		}
	}
	return 0
}
