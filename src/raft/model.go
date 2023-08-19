package raft

import (
	"encoding/json"
	"fmt"
)

// LogEntry log entry
type LogEntry struct {
	Data  []string
	Index uint64
	Term  int64
}

type NodeStatus int32

func (n *NodeStatus) String() {
	return
}

// CommonPresistState update on stable before respond rpc
type CommonPresistState struct {
	CurrentTerm int64       `json:"current_term"` // current term
	VoteFor     int32       `json:"vote_for"`     // candidateId in currentTerm, if the service is leader, votefor == me
	LogEntrys   []*LogEntry `json:"log_entrys"`   // log entries
}

// CommonVolatileState store in memory
type CommonVolatileState struct {
	commitIdx      uint64     // commit index
	lastAppliedIdx uint64     // applied index
	status         NodeStatus // leader flag
}

// LeaderVolatileState store in memory
type LeaderVolatileState struct {
	// most of time, nextIndex = matchIndex + 1
	nextIdx  []uint64 // for each service, means next send log's index,init to lastLogIndex + 1
	matchIdx []uint64 // for each service, means the log's index that service last receive , init to 0
}

// CandidateVolatileState store in memory
type CandidateVolatileState struct {
	totalVote   int32
	targetVote  int32
	currentVote int32
}

func NewCandidateVolatileState(count int32) *CandidateVolatileState {
	return &CandidateVolatileState{
		totalVote:   count,
		targetVote:  count/2 + 1,
		currentVote: 1,
	}
}

func (c *CandidateVolatileState) successCandidate() bool {
	if c.currentVote >= c.targetVote {
		return true
	}
	return false
}

func (c *CandidateVolatileState) reset() {
	c = nil
}

func (c *CandidateVolatileState) valid() bool {
	return c != nil
}

func (c *CandidateVolatileState) String() string {
	return fmt.Sprintf("totalVote:%d, targetVote:%d, currentVote:%d", c.totalVote, c.targetVote, c.currentVote)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int64  `json:"term"`
	CandidateID int32  `json:"candidate_id"`
	LastLogIdx  uint64 `json:"lastlog_idx"`
	LastLogTerm int64  `json:"lastlog_term"`
}

func (r *RequestVoteArgs) String() string {
	buf, _ := json.Marshal(r)
	return string(buf)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int64 `json:"term"`
	VoteGranted bool  `json:"vote_granted"`
}

func (r *RequestVoteReply) String() string {
	buf, _ := json.Marshal(r)
	return string(buf)
}

type AppendEntriesArgs struct {
	Term     int64 `json:"term"`
	LeaderId int32 `json:"leader_id"`
}

func (a *AppendEntriesArgs) String() string {
	buf, _ := json.Marshal(a)
	return string(buf)
}

type AppendEntriesReply struct {
	Term    int64 `json:"term"`
	Success bool  `json:"success"`
}

func (a *AppendEntriesReply) String() string {
	buf, _ := json.Marshal(a)
	return string(buf)
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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
