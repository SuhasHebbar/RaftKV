package kv

import (
	"context"
	"math/rand"
	"strconv"
	"sync"
	"time"

	pb "github.com/SuhasHebbar/CS739-P2/proto"
)

// const Amp = 50
const Amp = 1

// Election timeouts in milliseconds
const MIN_ELECTION_TIMEOUT = 150 * Amp
const MAX_ELECTION_TIMEOUT = 300 * Amp

const RPC_TIMEOUT = 10 * time.Second * Amp

const VOTE_FILE_TEMPLATE = "raftvotes"
const LOG_FILE_TEMPLATE = "raftlogs"

const (
	FOLLOWER  = "FOLLOWER"
	CANDIDATE = "CANDIDATE"
	LEADER    = "LEADER"
)

type PeerId = int32

const NIL_PEER = -1

type Raft struct {
	mu sync.Mutex
	id PeerId
	// Set of Peers
	peers map[PeerId]Empty
	role  string

	// Volatile state on all servers:
	commitIndex int32
	lastApplied int32

	// Volatile state on leaders
	nextIndex  map[PeerId]int32
	matchIndex map[PeerId]int32

	// Persistent state on all servers
	currentTerm int32
	votedFor    PeerId
	log         []*pb.LogEntry

	leaderId   PeerId
	rpcCh      chan RpcCommand
	commitCh   chan CommittedOperation
	rpcHandler RpcServer

	// volatile follower states.
	heartBeatTimeout time.Duration
	heartBeatTimer   *time.Timer

	p *Persistence
	voteFileName string
	logFileName string
}

func NewRaft(addr PeerId, peers map[PeerId]Empty, rpcHandler RpcServer) *Raft {
	nextIndex := map[PeerId]int32{}
	matchIndex := map[PeerId]int32{}

	for peer := range peers {
		nextIndex[peer] = 0
		matchIndex[peer] = -1
	}

	voteFileName := getVoteFileName(addr)
	logFileName := getLogFileName(addr)

	p := &Persistence{}

	vote, err1 := p.ReadVote(voteFileName)
	logs, _ := p.ReadLog(logFileName)

	if err1 != nil {
		vote = &pb.StoredVote{Term: 0, VotedFor: -1}
	}

	p.StoredVote = vote
	p.StoredLogs = logs

	return &Raft{
		id:    addr,
		peers: peers,
		role:  FOLLOWER,

		commitIndex: -1,
		lastApplied: -1,

		nextIndex:  nextIndex,
		matchIndex: matchIndex,

		currentTerm: vote.Term,
		votedFor:    vote.VotedFor,
		log:         logs.Logs,

		leaderId:   NIL_PEER,
		rpcCh:      make(chan RpcCommand),
		commitCh:   make(chan CommittedOperation),
		rpcHandler: rpcHandler,

		heartBeatTimeout: -1,
		heartBeatTimer:   nil,

		p: p,
		voteFileName: voteFileName,
		logFileName: logFileName,
	}
}

func (r *Raft) peersSize() int {
	return len(r.peers)
}

func (r *Raft) minimumVotes() int {
	return (r.peersSize() / 2) + 1
}

func (r *Raft) Debug(msg string, args ...any) {
	Debugf(strconv.Itoa(int(r.id))+": "+msg, args...)
}

func (r *Raft) Info(msg string, args ...any) {
	Infof(strconv.Itoa(int(r.id))+": "+msg, args...)
}

func (r *Raft) lastLogDetails() (int32, int32) {

	lastLogIndex := len(r.log) - 1

	lastLogTerm := int32(-1)
	if lastLogIndex >= 0 {
		lastLogTerm = r.log[lastLogIndex].Term

	}

	return int32(lastLogIndex), lastLogTerm
}

func (r *Raft) broadcastVoteRequest() <-chan *pb.RequestVoteReply {
	savedCurrentTerm := r.currentTerm
	votesCh := make(chan *pb.RequestVoteReply, r.peersSize())

	lastLogIndex, lastLogTerm := r.lastLogDetails()

	voteReq := &pb.RequestVoteRequest{
		Term:         r.currentTerm,
		CandidateId:  r.id,
		LastLogTerm:  lastLogTerm,
		LastLogIndex: int32(lastLogIndex),
	}

	for peerIdEx := range r.peers {
		peerId := peerIdEx
		// No need to broadcast vote request to self.
		if peerId == r.id {
			continue
		}

		go func() {

			rpcClient := r.rpcHandler.GetClient(peerId)
			r.Debug("Sending vote for term %v to peer %v", savedCurrentTerm, peerId)

			ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT)
			defer cancel()
			voteRes, err := rpcClient.RequestVote(ctx, voteReq)

			if err != nil {
				r.Debug("Failed to call RequestVote for term: %v, with error: %v", savedCurrentTerm, err)
				votesCh <- &pb.RequestVoteReply{VoteGranted: false, Term: savedCurrentTerm}
				return
			}

			votesCh <- voteRes
		}()

	}

	return votesCh

}

// Reset heartbeat timer to hearbeat timeout
func (r *Raft) resetHeartBeatTimer() {
	if r.heartBeatTimer == nil {
		return
	}

	if !r.heartBeatTimer.Stop() {
		<-r.heartBeatTimer.C
	}

	r.heartBeatTimer.Reset(r.heartBeatTimeout)
}

type RpcCommand struct {
	Command any
	resp    chan any
}

func (r *Raft) handleAppendEntries(req RpcCommand, appendReq *pb.AppendEntriesRequest) {
	r.Debug("Received AppendEntries: term: %v, leaderId: %v, prevLogIndex: %v, prevLogTerm: %v, leaderCommit: %v", appendReq.Term, appendReq.LeaderCommit, appendReq.PrevLogIndex, appendReq.PrevLogTerm, appendReq.LeaderCommit)

	entries := appendReq.Entries


	if appendReq.Term > r.currentTerm {
		r.becomeFollower(appendReq.Term, appendReq.LeaderId)
	}

	appendRes := &pb.AppendEntriesResponse{}
	appendRes.Success = false
	appendRes.Term = r.currentTerm
	appendRes.PeerId = r.id

	if appendReq.Term < r.currentTerm {
		return
	}

	if r.role != FOLLOWER {
		r.becomeFollower(appendReq.Term, appendReq.LeaderId)
	}

	r.resetHeartBeatTimer()
	// r.Debug("Entry size being pushed is %v", len(entries))

	// if len(entries) > 0 {
	// 	r.Debug("Non zero log entry to be pushed!")
	// 	for _, val := range entries {
	// 		r.Debug("This entry has term %v", val.Term)
	// 	}
	// }
	if appendReq.PrevLogIndex == -1 ||
		int(appendReq.PrevLogIndex) < len(r.log) && appendReq.PrevLogTerm == r.log[appendReq.PrevLogIndex].Term {
		appendRes.Success = true
		appendRes.Term = appendReq.Term

		logInsertOffset := int(appendReq.PrevLogIndex) + 1
		entriesOffset := 0

		for logInsertOffset < len(r.log) && entriesOffset < len(entries) {
			if r.log[logInsertOffset].Term != entries[entriesOffset].Term {
				break
			}
			logInsertOffset++
			entriesOffset++
		}

		if entriesOffset < len(entries) {
			r.Debug("Inserting entries to log. %v entries total inserter", len(entries)-entriesOffset)
			r.log = append(r.log[:logInsertOffset], entries[entriesOffset:]...)
			// persist log entries
			r.persistLogs()
		}

		// r.Debug("leadercommit: %v, localcommitindex: %v", appendReq.LeaderCommit, r.commitIndex)
		if appendReq.LeaderCommit > r.commitIndex {
			oldCommitIndex := r.commitIndex
			r.commitIndex = min32(appendReq.LeaderCommit, int32(len(r.log)-1))
			r.Debug("Commit index changing from %v to %v", oldCommitIndex, r.commitIndex)
			r.applyRange(oldCommitIndex+1, r.commitIndex)
		}

	}

	req.resp <- appendRes

}

func (r *Raft) handleRequestVoteRequest(req RpcCommand, voteReq *pb.RequestVoteRequest) {
	r.Debug("Received RequestVote term: %v, candidateId: %v, lastLogIndex: %v, lastLogTerm: %v", voteReq.Term, voteReq.CandidateId, voteReq.LastLogIndex, voteReq.LastLogTerm)

	if voteReq.Term > r.currentTerm {
		r.Debug("Becoming follower. term out of date")
		r.becomeFollower(voteReq.Term, voteReq.CandidateId)
	}

	lastLogIndex, lastLogTerm := r.lastLogDetails()

	voteRes := &pb.RequestVoteReply{
		Term:        r.currentTerm,
		VoteGranted: false,
		PeerId:      r.id,
	}

	if voteReq.Term == r.currentTerm && (r.votedFor == -1 || r.votedFor == voteReq.CandidateId) && ((voteReq.LastLogTerm > lastLogTerm) || (voteReq.LastLogTerm == lastLogTerm && voteReq.LastLogIndex >= lastLogIndex)) {
		voteRes.VoteGranted = true
		r.votedFor = voteReq.CandidateId
		// persist votedFor and term
		r.persistVotes()
		r.resetHeartBeatTimer()
		r.Debug("Successful vote to %v", r.votedFor)
	}

	req.resp <- voteRes
}

func (r *Raft) handleSubmitOperation(req RpcCommand) {
	r.Debug("Handling submit operation for term %v and logIndex: %v", r.currentTerm, len(r.log)-1)
	var pendingOperation PendingOperation
	pendingOperation.currentLeader = r.leaderId
	if r.role != LEADER {
		pendingOperation.isLeader = false
	} else {
		op, ok := req.Command.(*pb.Operation)
		if !ok {
			panic("Received no Operation type")
		}
		r.log = append(r.log, &pb.LogEntry{Term: r.currentTerm, Operation: op})
		r.persistLogs()

		pendingOperation.isLeader = true
		pendingOperation.logIndex = int32(len(r.log)) - 1
	}

	req.resp <- pendingOperation
}

func (r *Raft) handleRpc(req RpcCommand) {
	r.Debug("Handling rpc command.")
	switch v := req.Command.(type) {
	case *pb.AppendEntriesRequest:
		r.handleAppendEntries(req, v)
	case *pb.RequestVoteRequest:
		r.handleRequestVoteRequest(req, v)
	default:
		r.handleSubmitOperation(req)
	}
}

type appendEntriesData struct {
	request    *pb.AppendEntriesRequest
	response   *pb.AppendEntriesResponse
	numEntries int32
}

type safeN1Channel struct {
	C       chan *appendEntriesData
	closeCh chan Empty
}

func (r *Raft) broadcastAppendEntries(appendCh safeN1Channel) {
	savedCurrentTerm := r.currentTerm

	for peer := range r.peers {
		peerId := peer
		if peerId == r.id {
			continue
		}

		prevLogIndex := r.nextIndex[peerId] - 1
		prevLogTerm := int32(-1)
		if prevLogIndex >= 0 {
			prevLogTerm = r.log[prevLogIndex].Term
		}

		entries := r.log[r.nextIndex[peerId]:]
		numEntries := len(entries)

		appendReq := &pb.AppendEntriesRequest{
			Term:         savedCurrentTerm,
			LeaderId:     r.id,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: r.commitIndex,
		}

		go func() {
			client := r.rpcHandler.GetClient(peerId)

			ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT)
			defer cancel()

			r.Debug("Sending append for term: %v, leaderId: %v, prevLogIndex: %v, prevLogTerm: %v, leaderCommit: %v", appendReq.Term, appendReq.LeaderId, appendReq.PrevLogIndex, appendReq.PrevLogTerm, appendReq.LeaderCommit)

			resp, err := client.AppendEntries(ctx, appendReq)
			if err != nil {
				r.Debug("Failed to send AppendEntries for term: %v, prevLogIndex: %v, prevLogTerm: %v, commitIndex: %v, err: %v", savedCurrentTerm, prevLogIndex, prevLogTerm, appendReq.LeaderCommit, err)
				return
			}

			r.Debug("Received AppendEntries response.")

			select {
			case appendCh.C <- &appendEntriesData{request: appendReq, response: resp, numEntries: int32(numEntries)}:
			case <-appendCh.closeCh:
			}

		}()

	}
}

func (r *Raft) setRole(newRole string) {
	r.Debug("Changing role from %v to %v", r.role, newRole)
	if r.role == newRole {
		return
	}

	r.role = newRole
}

func (r *Raft) becomeFollower(term int32, leader PeerId) {
	r.setRole(FOLLOWER)
	r.currentTerm = term
	r.votedFor = -1
	r.leaderId = NIL_PEER
	// persist votedFor and term
	r.persistVotes()
}

func (r *Raft) handleAppendEntriesResponse(appendDat *appendEntriesData) {
	req := appendDat.request
	res := appendDat.response
	// r.Debug("Received AppendEntries response from %v. currentTerm: %v, reqTerm: %v, resTerm: %v", appendDat.response.PeerId, r.currentTerm, req.Term, res.Term)
	if req.Term != r.currentTerm || res.Term < r.currentTerm {
		return
	}

	if res.Term > r.currentTerm {
		r.Debug("currentTerm: %v, newTerm: %v", r.currentTerm, res.Term)
		r.becomeFollower(res.Term, res.PeerId)
		return
	}

	if res.Success {
		r.Debug("Got appendEntries reply from %v with old matchIndex: %v, nextIndex: %v", res.PeerId, r.matchIndex[res.PeerId], r.nextIndex[res.PeerId])
		newMatchIndex := req.PrevLogIndex + appendDat.numEntries
		// r.Debug("newMatchIndex: %v", newMatchIndex)
		if newMatchIndex > r.matchIndex[res.PeerId] {
			r.matchIndex[res.PeerId] = newMatchIndex
		}

		r.nextIndex[res.PeerId] = newMatchIndex + 1
		r.Debug("Got appendEntries reply from %v with new matchIndex: %v, nextIndex: %v", res.PeerId, r.matchIndex[res.PeerId], r.nextIndex[res.PeerId])
		oldCommitIndex := r.commitIndex
		for i := newMatchIndex; i >= oldCommitIndex && i >= 0; i-- {
			if r.log[i].Term != r.currentTerm {
				continue
			}
			// We're trying to advance the commit index for the log
			matches := 1
			for peerId := range r.peers {
				if peerId == r.id {
					continue
				}

				if r.matchIndex[peerId] >= i {
					matches++
				}

			}

			// r.Debug("For index %v we have %v matches", i, matches)
			if matches > r.peersSize()/2 {
				r.commitIndex = i
				r.applyRange(oldCommitIndex+1, i)
				break
			}

		}
	} else {
		// TODO: Not completely sure how to handle things here...
		r.nextIndex[res.PeerId] = req.PrevLogIndex
	}

}

type CommittedOperation struct {
	Operation *pb.Operation
	Index     int32
}

func (r *Raft) applyRange(a, b int32) {
	for j := a; j <= b; j++ {
		// r.Debug("Applying operation for log %v", j)
		operation := r.log[j].Operation

		// Empty operations do not need to be applied to the state machine
		if operation.Type == pb.OperationType_NOOP {
			continue
		}
		r.commitCh <- CommittedOperation{Operation: operation, Index: j}
	}
	r.lastApplied = b

}

func (r *Raft) runAsLeader() {
	r.Debug("Running as leader for term %v.", r.currentTerm)

	nextIndex := map[PeerId]int32{}
	matchIndex := map[PeerId]int32{}

	for peer := range r.peers {
		nextIndex[peer] = int32(len(r.log))
		matchIndex[peer] = -1
	}

	appendCh := safeN1Channel{
		C:       make(chan *appendEntriesData, r.peersSize()),
		closeCh: make(chan Empty),
	}
	defer close(appendCh.closeCh)

	dummyEntry := &pb.LogEntry{
		Term:      r.currentTerm,
		Operation: &pb.Operation{Type: pb.OperationType_NOOP},
	}

	// Add dummy entry to ensure previous term entries are commited on followers.
	r.log = append(r.log, dummyEntry)
	// persist log entries
	r.persistLogs()

	// Call it in the beginning to ensure heartbeat is sent.
	r.broadcastAppendEntries(appendCh)

	heartbeatTimer := time.After(getLeaderLease())
	for r.role == LEADER {
		select {
		case req := <-r.rpcCh:
			r.handleRpc(req)
		case <-heartbeatTimer:
			r.broadcastAppendEntries(appendCh)
			heartbeatTimer = time.After(getLeaderLease())
		case appendRes := <-appendCh.C:
			r.handleAppendEntriesResponse(appendRes)
		}
	}

}

func (r *Raft) runAsCandidate() {
	r.currentTerm++

	electionTimout := getElectionTimeout()
	electionTimer := time.NewTimer(electionTimout)
	defer electionTimer.Stop()

	votesCh := r.broadcastVoteRequest()

	targetVotes := r.minimumVotes()
	// Vote for self
	currentVotes := 1
	r.votedFor = r.id


	r.persistVotes()

	for r.role == CANDIDATE {
		select {
		case req := <-r.rpcCh:
			r.handleRpc(req)
		case <-electionTimer.C:
			// We restart the election
			return
		case vote := <-votesCh:
			if vote.Term > r.currentTerm {
				r.becomeFollower(vote.Term, vote.PeerId)
				r.Debug("Newer term. Fallback to follower")
				return
			}
			if vote.VoteGranted {
				currentVotes++
				r.Debug("Received vote from: %v, term: %v, currentVotes: %v, total: %v", vote.PeerId, r.currentTerm, currentVotes, targetVotes)
			}

			if currentVotes >= targetVotes {
				r.Debug("Won election for term: %v, currentVotes: %v", r.currentTerm, currentVotes)
				r.setRole(LEADER)
				r.leaderId = r.id
				return
			}

		}
	}

}

func (r *Raft) persistVotes() {
	r.p.StoredVote.Term = r.currentTerm
	r.p.StoredVote.VotedFor = r.votedFor
	r.p.WriteVote(r.voteFileName)
	r.Debug("Persisted Votes")
}

func (r *Raft) persistLogs() {
	startTime := time.Now()
	r.p.StoredLogs.Logs = r.log
	r.p.WriteLog(r.logFileName)
	r.Debug("Persisted Logs")
	r.Info("persist time::", "latency", time.Since(startTime))
}



func (r *Raft) runAsFollower() {
	r.Debug("Running a follower.")

	r.heartBeatTimeout = getHeartbeatTimeout()
	r.heartBeatTimer = time.NewTimer(r.heartBeatTimeout)
	defer func() {
		r.heartBeatTimer.Stop()
		r.heartBeatTimer = nil
		r.heartBeatTimeout = -1
	}()

	for {
		select {
		case req := <-r.rpcCh:
			// do nothing for now.
			r.handleRpc(req)
		case <-r.heartBeatTimer.C:
			r.setRole(CANDIDATE)
			r.leaderId = NIL_PEER
			return
		}
	}

}

// The server loop is implemented as a state machine where all operations are serialised into
// a single thread of execution using channels.
func (r *Raft) startServerLoop() {
	for {
		r.Debug("Running server loop")
		switch r.role {
		case FOLLOWER:
			r.runAsFollower()
		case CANDIDATE:
			r.runAsCandidate()
		case LEADER:
			r.runAsLeader()

		}

	}

}

func getRandomTimer() <-chan time.Time {
	randomTimeout := getElectionTimeout()

	// timer for random timeout duration after Now()
	return time.After(randomTimeout)
}

func getLeaderLease() time.Duration {
	return time.Duration(10000 * time.Microsecond)
	// return time.Duration(MIN_ELECTION_TIMEOUT * time.Millisecond / 3)
}

func getElectionTimeout() time.Duration {
	return getRandomTimeout(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT)
}

func getHeartbeatTimeout() time.Duration {
	return getRandomTimeout(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT)
}

func getRandomTimeout(minTimeout, maxTimeout int) time.Duration {
	return time.Duration((minTimeout +
		rand.Intn(1+maxTimeout-minTimeout)) * int(time.Millisecond),
	)
}

func getVoteFileName(id PeerId) string {
	return VOTE_FILE_TEMPLATE + strconv.Itoa(int(id))
}

func getLogFileName(id PeerId) string {
	return LOG_FILE_TEMPLATE + strconv.Itoa(int(id))
}
