package raft

import (
	"fmt"
	"log"
	"time"
)

// Debugging
const Debug = false

var DebugTime time.Time

func Init() {
	DebugTime = time.Now()
}

func getCurrentTimeStamp() string {
	time := time.Since(DebugTime).Microseconds()
	time /= 100
	return fmt.Sprintf("%06d ", time)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		format = getCurrentTimeStamp() + format
		log.Printf(format, a...)
	}
	return
}

func Fatalf(format string, a ...interface{}) (n int, err error) {
	format = getCurrentTimeStamp() + format
	log.Fatalf(format, a...)
	return
}
