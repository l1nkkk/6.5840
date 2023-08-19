package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"github.com/l1nkkk/6.5840/labgob"
	"github.com/l1nkkk/6.5840/src/labrpc"
	"github.com/l1nkkk/6.5840/src/lin_util/lin_log"
)

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	// 初始化，不需要加锁避免竞争
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = int32(me)
	rf.applyCh = applyCh
	rf.candidateCh = make(chan struct{}, 0)

	rf.commomPresistState = &CommonPresistState{
		CurrentTerm: 0,
		VoteFor:     VoteNone,
		LogEntrys:   nil,
	}

	// TODO 暂时不需要
	rf.commomVolatileState = &CommonVolatileState{
		commitIdx:      0,
		lastAppliedIdx: 0,
		status:         FollowerStatus,
	}

	rf.leaderVolatileState = &LeaderVolatileState{
		nextIdx:  nil,
		matchIdx: nil,
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 简化，重启直接算是follower
	rf.initTicker()
	rf.becomeFollower(rf.term(), rf.commomPresistState.VoteFor)

	// if rf.commomPresistState.VoteFor == rf.me {
	// 	rf.becomeLeader()
	// 	// TODO 暂时不需要
	// 	rf.leaderVolatileState = &LeaderVolatileState{}
	// } else {
	// 	rf.becomeFollower()
	// }
	lin_log.Info("[Raft] [%d] [Make], raft_details:[%s]", rf.node(), rf)
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // [Unlock]Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // [Unlock]RPC end points of all peers
	me        int32               // [Unlock]this peer's index into peers[]
	dead      int32               // [Unlock]set by Kill()
	persister *Persister          // [Lock]Object to hold this peer's persisted state

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	commomPresistState     *CommonPresistState     // [Lock]
	commomVolatileState    *CommonVolatileState    // [Lock]
	leaderVolatileState    *LeaderVolatileState    // [Lock]
	candidateVolatileState *CandidateVolatileState // [Lock]

	electionTicker               *time.Ticker // [Lock]
	candidateProgressCheckTicker *time.Ticker // [Lock]
	heartbeatTicker              *time.Ticker // [Lock]

	applyCh     chan ApplyMsg
	candidateCh chan struct{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return int(rf.commomPresistState.CurrentTerm), rf.isLeader()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	lin_log.Info("[Raft] [%d] [Kill], raft_details:[%s]", rf.node(), rf)
}

// RequestVote [RPC].
func (rf *Raft) PreRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.requestVoteHandle(args, reply, true)
}

// RequestVote [RPC].
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.requestVoteHandle(args, reply, false)
}

// AppendEntries [RPC].
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// TODO 暂时支持心跳
	rf.mu.Lock()
	defer rf.mu.Unlock()
	situtation := 0
	if args.Term > rf.term() {
		rf.becomeFollower(args.Term, args.LeaderId)
		reply.Term, reply.Success = rf.term(), true
		situtation = 1
	} else if args.Term == rf.term() {
		if rf.isLeader() {
			lin_log.Fatal("[Raft] [%d] [RPC] [AppendEntries] raft_details:[%s], args:[%v], reply:[%v]", rf.node(), rf, args, reply)
		}
		rf.resetElectionTimer()
		reply.Term, reply.Success = rf.term(), true
		situtation = 2
	} else {
		reply.Term, reply.Success = rf.term(), false
		situtation = 3
	}
	lin_log.Debug("[Raft] [%d] [RPC] [AppendEntries], situtation:[%d], raft_details:[%s], args:[%v], reply:[%v]", rf.node(), situtation, rf, args, reply)
}

// RequestVote [RPC].
func (rf *Raft) requestVoteHandle(args *RequestVoteArgs, reply *RequestVoteReply, isPre bool) {
	funcName := "RequestVote"
	if isPre {
		funcName = "PreRequestVote"
	}
	// Your code here (2A, 2B).
	situtation := 0
	if args.Term < rf.commomPresistState.CurrentTerm {
		// 1. check term
		reply.Term, reply.VoteGranted = rf.term(), false
		situtation = 1
	} else if args.Term == rf.commomPresistState.CurrentTerm {
		if rf.commomPresistState.VoteFor == VoteNone {
			// 2. has not voted in this term, this situation is unexpected
			lin_log.Fatal("[Raft] [RPC] [%s] [%d], raft_details:[%s], args:[%s], reply:[%s]", funcName, rf.node(), rf, args, reply)
		} else if rf.commomPresistState.VoteFor != args.CandidateID {
			// 3. has voted to other candidate
			reply.Term, reply.VoteGranted = rf.term(), false
			situtation = 3
		} else if rf.commomPresistState.VoteFor == args.CandidateID {
			// 4. recall by the same service
			reply.Term, reply.VoteGranted = rf.term(), true
			situtation = 4
		}
	} else if args.Term > rf.commomPresistState.CurrentTerm {
		if args.LastLogTerm < rf.lastLogTerm() || args.LastLogIdx < rf.lastLogIndex() {
			// 5. check lastLogterm and lastLogIndex
			reply.Term, reply.VoteGranted = rf.term(), false
			situtation = 5
		} else {
			// 6. successfully
			if !isPre {
				rf.becomeFollower(args.Term, args.CandidateID)
			}

			reply.Term, reply.VoteGranted = args.Term, true
			situtation = 6
		}
	} else {
		lin_log.Fatal("[Raft] [%d] [RPC] [%s], raft_details:[%s], args:[%s], reply:[%s]", rf.node(), funcName, rf, args, reply)
	}
	lin_log.Info("[Raft] [%d] [RPC] [%s], situtation:[%d], raft_details:[%s], args:[%s], reply:[%s]", rf.node(), funcName, situtation, rf, args, reply)
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (2A)
		killChecker := time.NewTicker(time.Duration(50+(rand.Int63()%300)) * time.Microsecond)
		// Check if a leader election should be started.
		select {
		case <-killChecker.C:
		case <-rf.electionTicker.C:
			func() {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.isLeader() {
					lin_log.Fatal("[Raft] [%d] [ticker] [ElectionTimeout] raft:[%s], leader -> precandidate", rf.node(), rf)
				} else if rf.isCandidate() {
					rf.becomePreCandidate()
					lin_log.Info("[Raft] [%d] [ticker] [ElectionTimeout] [re-start] [%s]", rf.node(), rf)
				} else {
					rf.becomePreCandidate()
					lin_log.Info("[Raft] [%d] [ticker] [ElectionTimeout] [start] [%s]", rf.node(), rf)
				}

				for i, _ := range rf.peers {
					if i == int(rf.me) {
						continue
					}
					go func(currentNode int) {
						req := &RequestVoteArgs{
							Term:        rf.term() + 1, // [important]
							CandidateID: rf.node(),
							LastLogIdx:  rf.lastLogIndex(),
							LastLogTerm: rf.lastLogTerm(),
						}
						reply := &RequestVoteReply{}
						if rf.sendPreRequestVote(currentNode, req, reply) {
							rf.mu.Lock()
							if reply.VoteGranted && reply.Term == rf.term()+1 && rf.candidateVolatileState.valid() {
								rf.candidateVolatileState.currentVote++
								lin_log.Debug("[Raft] [%d] [ticker] [sendPreRequestVote] [resp from %d success]  candidateVolatileState:[%s]", rf.node(), currentNode, rf.candidateVolatileState)
							}
							rf.mu.Unlock()
						}
					}(i)
				}
			}()
		case <-rf.candidateProgressCheckTicker.C:
			func() {
				rf.mu.Lock()
				if rf.isCandidate() {
					if rf.candidateVolatileState.successCandidate() {
						rf.becomeLeader()
						lin_log.Info("[Raft] [%d] [ticker] [CandidateProgressCheck] [Become Leader] raft:[%s], candidateVolatileState:[%s]", rf.node(), rf, rf.candidateVolatileState)
					}
				} else if rf.isPreCandidate() {
					if rf.candidateVolatileState.successCandidate() {
						go func() { // important, note for dead lock
							rf.candidateCh <- struct{}{}
						}()
						lin_log.Info("[Raft] [%d] [ticker] [CandidateProgressCheck] [PreCandidate Done] raft:[%s], candidateVolatileState:[%s]", rf.node(), rf, rf.candidateVolatileState)
					}
				}
				rf.mu.Unlock()
			}()
		case <-rf.candidateCh:
			func() {
				rf.mu.Lock()
				rf.becomeCandidate()
				rf.mu.Unlock()
				for i, _ := range rf.peers {
					if i == int(rf.me) {
						continue
					}
					go func(currentNode int) {
						rf.mu.Lock()
						req := &RequestVoteArgs{
							Term:        rf.term(), // [important]
							CandidateID: rf.node(),
							LastLogIdx:  rf.lastLogIndex(),
							LastLogTerm: rf.lastLogTerm(),
						}
						rf.mu.Unlock()
						reply := &RequestVoteReply{}
						if rf.sendRequestVote(currentNode, req, reply) {
							rf.mu.Lock()
							if reply.VoteGranted && reply.Term == rf.term() && rf.candidateVolatileState.valid() {
								rf.candidateVolatileState.currentVote++
								lin_log.Debug("[Raft] [%d] [ticker] [sendRequestVote] [receive success]  candidateVolatileState:[%s]", rf.node(), rf.candidateVolatileState)
							}
							rf.mu.Unlock()
						}
					}(i)
				}
			}()
		case <-rf.heartbeatTicker.C:
			func() {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if !rf.isLeader() {
					lin_log.Fatal("[Raft] [%d] [ticker] [heartbeatTicker]  raft:[%s] unexpected situtation", rf.node(), rf)
				}
				rf.broadcast()
			}()
		}

		// [l1nkkk] gloss by me
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) String() string {
	return fmt.Sprintf("node: <%d>, nodestatus: <%v>, term: <%d>, votefor: <%d>",
		rf.me, rf.status(),
		rf.commomPresistState.CurrentTerm, rf.commomPresistState.VoteFor)
}

func (rf *Raft) node() int32 {
	return rf.me
}

func (rf *Raft) lastLogIndex() uint64 {
	if len(rf.commomPresistState.LogEntrys) == 0 {
		return 0
	}
	return rf.commomPresistState.LogEntrys[len(rf.commomPresistState.LogEntrys)-1].Index
}

func (rf *Raft) lastLogTerm() int64 {
	if len(rf.commomPresistState.LogEntrys) == 0 {
		return 0
	}
	return rf.commomPresistState.LogEntrys[len(rf.commomPresistState.LogEntrys)-1].Term
}

func (rf *Raft) becomePreCandidate() {
	rf.setStatus(PreCandidateStatus)
	rf.resetCandidateVolatileStat()
	// ticker init
	rf.resetElectionTimer()
	rf.candidateProgressCheckTicker.Reset(CandidateProgressCheckTimeout)
	rf.heartbeatTicker.Stop()
}

func (rf *Raft) becomeCandidate() {
	rf.commomPresistState.VoteFor = rf.me
	rf.setTerm(rf.term() + 1)
	rf.setStatus(CandidateStatus)
	rf.persist()
	rf.resetCandidateVolatileStat()
	// ticker init
	rf.resetElectionTimer()
	rf.candidateProgressCheckTicker.Reset(CandidateProgressCheckTimeout)
	rf.heartbeatTicker.Stop()
}

func (rf *Raft) becomeFollower(newTerm int64, voteFor int32) {
	rf.setTerm(newTerm)
	rf.setStatus(FollowerStatus)
	rf.commomPresistState.VoteFor = voteFor
	rf.persist()

	// ticker init
	rf.resetElectionTimer()
	rf.candidateProgressCheckTicker.Stop()
	rf.heartbeatTicker.Stop()
}

func (rf *Raft) becomeLeader() {
	if !rf.isLeader() {
		rf.candidateVolatileState = nil
		rf.setStatus(LeaderStatus)
		rf.persist()
	}
	// ticker init
	rf.electionTicker.Stop()
	rf.heartbeatTicker.Reset(HeartbeatTimeout)
	rf.candidateProgressCheckTicker.Stop()
	rf.broadcast()
}

// broadcast send HeadBeat to every client
func (rf *Raft) broadcast() {
	// [l1nkkk] note for define before go func()
	req := &AppendEntriesArgs{
		Term:     rf.term(),
		LeaderId: rf.me,
	}
	for i, _ := range rf.peers {
		if i == int(rf.me) {
			continue
		}
		go func(target int) {
			reply := &AppendEntriesReply{}
			rf.sendAppendEntries(target, req, reply)
		}(i)
	}
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTicker.Reset(time.Duration(600+(rand.Int63()%200)) * time.Millisecond)
}

func (rf *Raft) resetCandidateVolatileStat() {
	rf.candidateVolatileState = NewCandidateVolatileState(int32(len(rf.peers)))
}

// initTicker init ticker
func (rf *Raft) initTicker() {
	rf.electionTicker = time.NewTicker(time.Duration(600+(rand.Int63()%200)) * time.Millisecond)
	rf.candidateProgressCheckTicker = time.NewTicker(CandidateProgressCheckTimeout)
	rf.heartbeatTicker = time.NewTicker(HeartbeatTimeout)
	rf.electionTicker.Stop()
	rf.candidateProgressCheckTicker.Stop()
	rf.heartbeatTicker.Stop()
}

// [l1nkkk] use json directly

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	raftState, _ := json.Marshal(rf.commomPresistState)
	rf.persister.Save(raftState, nil)
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	rf.commomPresistState = &CommonPresistState{}
	json.Unmarshal(data, rf.commomPresistState)
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
func (rf *Raft) sendPreRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call(PreRequestVoteRPC, args, reply)
	return ok
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call(RequestVoteRPC, args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call(AppendEntriesRPC, args, reply)
	return ok
}

func (rf *Raft) isLeader() bool {
	return rf.status() == LeaderStatus
}

func (rf *Raft) isPreCandidate() bool {
	return rf.status() == PreCandidateStatus
}

func (rf *Raft) isCandidate() bool {
	return rf.status() == CandidateStatus
}

func (rf *Raft) isFollower() bool {
	return rf.status() == FollowerStatus
}

func (rf *Raft) term() int64 {
	return rf.commomPresistState.CurrentTerm
}

func (rf *Raft) setTerm(t int64) {
	rf.commomPresistState.CurrentTerm = t
}

func (rf *Raft) status() NodeStatus {
	return rf.commomVolatileState.status
}

func (rf *Raft) setStatus(newStatus NodeStatus) {
	rf.commomVolatileState.status = newStatus
}
