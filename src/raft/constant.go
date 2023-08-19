package raft

import (
	"time"
)

const (
	PreRequestVoteRPC = "Raft.PreRequestVote"
	RequestVoteRPC    = "Raft.RequestVote"
	AppendEntriesRPC  = "Raft.AppendEntries"
)

const (
	VoteNone int32 = -1
)

var (
	// ElectionTimeout               = time.Duration(600+(rand.Int63()%200)) * time.Millisecond
	// KillCheckerTimeout            = time.Duration(50+(rand.Int63()%300)) * time.Microsecond
	CandidateProgressCheckTimeout = time.Duration(30) * time.Millisecond

	// The tester requires that the leader send heartbeat RPCs no more than ten times per second.
	HeartbeatTimeout = time.Duration(150) * time.Millisecond
)

const (
	LeaderStatus       NodeStatus = 1
	PreCandidateStatus NodeStatus = 2
	CandidateStatus    NodeStatus = 3
	FollowerStatus     NodeStatus = 4
)
