package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"math/rand"
	"sync"
	"time"

	pb "github.com/SuhasHebbar/CS739-P2/proto"
	"golang.org/x/exp/slog"
)

// Election timeouts in milliseconds
const MIN_ELECTION_TIMEOUT = 150
const MAX_ELECTION_TIMEOUT = 300

const RPC_TIMEOUT = 10 * time.Second

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
	log        []LogEntry

	leaderId   PeerId
	rpcCh      chan any
	commitCh chan any
	rpcHandler RpcServer
}

type LogEntry struct {
	Term      int32
	Operation any
}

func NewRaft(addr PeerId, peers map[PeerId]Empty, rpcHandler RpcServer) *Raft {
	nextIndex := map[PeerId]int32{}
	matchIndex := map[PeerId]int32{}

	for peer := range peers {
		nextIndex[peer] = 0
		matchIndex[peer] = -1
	}

	return &Raft{
		id:    addr,
		peers: peers,
		role:  FOLLOWER,

		commitIndex: -1,
		lastApplied: -1,

		nextIndex:  nextIndex,
		matchIndex: matchIndex,

		currentTerm: 0,
		votedFor:    NIL_PEER,
		log:        []LogEntry{},

		leaderId:   NIL_PEER,
		rpcCh:      make(chan any),
		commitCh: make(chan any),
		rpcHandler: rpcHandler,
	}
}

func (r *Raft) peersSize() int {
	return len(r.peers)
}

func (r *Raft) minimumVotes() int {
	return (r.peersSize() / 2) + 1
}

func (r *Raft) Debug(msg string, args ...any) {
	slog.Debug(string(r.id)+": "+msg, args...)
}

func (r *Raft) broadcastVoteRequest() <-chan *pb.RequestVoteReply {
	savedCurrentTerm := r.currentTerm
	votesCh := make(chan *pb.RequestVoteReply, r.peersSize())

	lastLogIndex := len(r.log) - 1
	lastLogTerm := r.log[lastLogIndex].Term

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

			ctx, _ := context.WithTimeout(context.Background(), RPC_TIMEOUT)
			voteRes, err := rpcClient.RequestVote(ctx, voteReq)

			if err != nil {
				r.Debug("Failed to call RequestVote for term: %v, with error: %v", r.currentTerm, err)
				votesCh <- &pb.RequestVoteReply{VoteGranted: false, Term: savedCurrentTerm}
				return
			}

			votesCh <- voteRes
		}()

	}

	return votesCh

}

func (r *Raft) handleRpc(req any) error {
	return nil
}

type appendEntriesData struct {
	request  *pb.AppendEntriesRequest
	response *pb.AppendEntriesResponse
	numEntries int32
}

type safeN1Channel struct {
	C      chan *appendEntriesData
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
		prevLogTerm := r.log[prevLogIndex].Term

		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		entries := r.log[r.nextIndex[peerId]:]
		numEntries := len(entries)
		if err := enc.Encode(entries); err != nil {
			r.Debug("Failed to encode. exiting")
			panic(err)
		}

		appendReq := &pb.AppendEntriesRequest{
			Term:         savedCurrentTerm,
			LeaderId:     r.id,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      buf.Bytes(),
			LeaderCommit: r.commitIndex,
		}

		go func() {
			client := r.rpcHandler.GetClient(peerId)

			ctx, _ := context.WithTimeout(context.Background(), RPC_TIMEOUT)
			resp, err := client.AppendEntries(ctx, appendReq)
			if err != nil {
				r.Debug("Failed to send AppendEntries for term: %v, prevLogIndex: %v, prevLogTerm: %v, commitIndex: %v", savedCurrentTerm, prevLogIndex, prevLogTerm, appendReq.LeaderCommit)
				return
			}

			select {
			case appendCh.C <- &appendEntriesData{request: appendReq, response: resp, numEntries: int32(numEntries)}:
				case <-appendCh.closeCh:
			}

		}()

	}
}

func (r *Raft) setRole(newRole string) {
	if r.role == newRole {
		return
	}

	r.Debug("Changing role from %v to %v", r.role, newRole)
	r.role = newRole
}

func (r *Raft) handleAppendResponse(appendDat *appendEntriesData) {
	req := appendDat.request
	res := appendDat.response
	if req.Term != r.currentTerm || res.Term < r.currentTerm {
		return
	}

	if res.Term > r.currentTerm {
		r.setRole(FOLLOWER)
		r.currentTerm = res.Term
		r.votedFor = -1
		return
	}

	if res.Success {
		newMatchIndex := req.PrevLogIndex + appendDat.numEntries
		if newMatchIndex > r.matchIndex[res.PeerId] {
			r.matchIndex[res.PeerId] = newMatchIndex
		}

		r.nextIndex[res.PeerId] = newMatchIndex + 1
		r.Debug("Got appendEntries repply from %v with new nextIndex %v", res.PeerId, r.nextIndex[res.PeerId])
		oldCommitIndex := r.commitIndex
		for i := newMatchIndex; i > oldCommitIndex; i-- {
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

			if matches > r.peersSize() / 2 {
				r.commitIndex = i
				for j := oldCommitIndex; j <= i; j++ {
					r.commitCh <- r.log[j].Operation
				}
				r.lastApplied = i
				break;
			}

		} 
	} else {
		// TODO: Not completely sure how to handle things here...
		r.nextIndex[res.PeerId] = req.PrevLogIndex
	}

}

func (r *Raft) runAsLeader() {
	r.Debug("Running as leader for term %v.", r.currentTerm)

	appendCh := safeN1Channel{
		C:      make(chan *appendEntriesData, r.peersSize()),
		closeCh: make(chan Empty),
	}

	// Call it in the beginning to ensure heartbeat is sent.
	r.broadcastAppendEntries(appendCh)
	for r.role == LEADER {
		select {
		case req := <-r.rpcCh:
			r.handleRpc(req)
		case <-time.After(getLeaderLease()):
			r.broadcastAppendEntries(appendCh)
		case appendRes := <-appendCh.C:
			r.handleAppendResponse(appendRes)
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

	for r.role == CANDIDATE {
		select {
		case req := <-r.rpcCh:
			r.handleRpc(req)
		case <-electionTimer.C:
			// We restart the election
			return
		case vote := <-votesCh:
			if vote.Term > r.currentTerm {
				r.setRole(FOLLOWER)
				r.currentTerm = vote.Term
				r.Debug("Newer term. Fallback to follower")
				return
			}
			if vote.VoteGranted {
				currentVotes++
				r.Debug("Received vote from: %v, term: %v, currentVotes: %v, total: %v", vote.PeerId, r.currentTerm, currentVotes, targetVotes)
			}

			if currentVotes >= targetVotes {
				r.Debug("Won election for term: %v, currentVotes: %v")
				r.setRole(LEADER)
				r.leaderId = r.id
				return
			}

		}
	}

}

func (r *Raft) runAsFollower() {

	heartBeatTimeout := getHeartbeatTimeout()

	heartBeatTimer := time.NewTimer(heartBeatTimeout)
	defer heartBeatTimer.Stop()

	for {
		select {
		case req := <-r.rpcCh:
			// do nothing for now.
			err := r.handleRpc(req)
			if err == nil {
				ResetTimer(heartBeatTimer, heartBeatTimeout)
			}
		case <-heartBeatTimer.C:
			r.setRole(CANDIDATE)
			return
		}
	}

}

// The server loop is implemented as a state machine where all operations are serialised into
// a single thread of execution using channels.
func (r *Raft) startServerLoop() {
	for {
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
	return time.Duration(MIN_ELECTION_TIMEOUT * time.Millisecond / 2)
}

func getElectionTimeout() time.Duration {
	return getRandomTimeout(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT)
}

func getHeartbeatTimeout() time.Duration {
	return getRandomTimeout(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT)
}

func getRandomTimeout(minTimeout, maxTimeout int) time.Duration {
	return time.Duration(minTimeout +
		rand.Intn(1+maxTimeout-minTimeout)*int(time.Millisecond),
	)
}
