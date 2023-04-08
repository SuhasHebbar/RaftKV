package kv

import (
	"context"
	"errors"
	"sync"
	"time"

	pb "github.com/SuhasHebbar/CS739-P2/proto"
	"golang.org/x/exp/slog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	empty "github.com/golang/protobuf/ptypes/empty"
	wrappers "github.com/golang/protobuf/ptypes/wrappers"
)

const REQUEST_TERMINATED = "Request was terminated."
const NOT_LEADER = "Server is not a leader."
const SIMULATED_PARTITION = "Simulated Partition"

type RaftRpcServer struct {
	raft       *Raft
	clients    map[PeerId]pb.RaftRpcClient
	kv         *KVStore
	pendingOps map[int32]chan *KVResult
	mu         sync.Mutex
	pb.UnimplementedRaftRpcServer
	config *Config
}

type RpcServer interface {
	GetClient(peerId PeerId) pb.RaftRpcClient
}

type PendingOperation struct {
	isLeader bool
	currentLeader PeerId
	logIndex int32
}

func NewRaftRpcServer(id PeerId, config *Config) *RaftRpcServer {
	peers := map[PeerId]Empty{}
	clients := map[PeerId]pb.RaftRpcClient{}

	for peerId := range config.Peers {
		peers[peerId] = Empty{}

		if peerId == id {
			// we do not need to contact ourselves via RPC
			clients[peerId] = nil
			continue
		}

		opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

		conn, err := grpc.Dial(config.Peers[peerId], opts...)
		if err != nil {
			slog.Error("Failed to dial", "err", err)
			panic(err)
		}

		clients[peerId] = pb.NewRaftRpcClient(conn)

	}
	self := &RaftRpcServer{}
	self.raft = NewRaft(id, peers, self)
	self.clients = clients
	self.kv = NewKVStore()
	self.pendingOps = map[int32]chan *KVResult{}
	self.config = config

	go func() {
		self.raft.startServerLoop()
	}()

	go self.startCommitListerLoop()
	return self
}

type KVResult struct {
	Value string
	Err error
}

func (rs *RaftRpcServer) startCommitListerLoop() {
	for {
		op := <-rs.raft.commitCh
		Debugf("Received operation for index %v", op.Index)
		kvop := op.Operation

		result := &KVResult{}
		if kvop.Type == pb.OperationType_GET {
			value, err := rs.kv.Get(kvop.Key)
			result.Value = value
			result.Err = err
		} else if kvop.Type == pb.OperationType_SET {
			rs.kv.Set(kvop.Key, kvop.Value)
		} else {
			err := rs.kv.Delete(kvop.Key)
			result.Err = err
		}

		rs.mu.Lock()
		// Infof("Returning response for %v", op.Operation)
		if rs.pendingOps[op.Index] == nil {
			rs.pendingOps[op.Index] = make(chan *KVResult, 1)
		}
		rs.pendingOps[op.Index] <- result

		rs.mu.Unlock()

	}

}

func (rs *RaftRpcServer) GetClient(peerId PeerId) pb.RaftRpcClient {
	return rs.clients[peerId]
}

func (rs *RaftRpcServer) RequestVote(ctx context.Context, in *pb.RequestVoteRequest) (*pb.RequestVoteReply, error) {
	if rs.config.Partitioned {
		<-ctx.Done()
		return nil, errors.New(SIMULATED_PARTITION)

	}

	cmd := RpcCommand{
		Command: in,
		resp:    make(chan any, 1),
	}

	rs.raft.rpcCh <- cmd

	resp, ok := (<-cmd.resp).(*pb.RequestVoteReply)
	if !ok {
		Debugf("Could not convert to RequestVoteReply")
		panic(ok)
	}

	return resp, nil
}

func (rs *RaftRpcServer) AppendEntries(ctx context.Context, in *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	if rs.config.Partitioned {
		<-ctx.Done()
		return nil, errors.New(SIMULATED_PARTITION)

	}

	cmd := RpcCommand{
		Command: in,
		resp:    make(chan any, 1),
	}

	rs.raft.rpcCh <- cmd

	resp, ok := (<-cmd.resp).(*pb.AppendEntriesResponse)
	if !ok {
		Debugf("Could not convert to AppendEntriesResponse")
		panic(ok)
	}

	return resp, nil
}

func (rs *RaftRpcServer) scheduleRpcCommand(ctx context.Context, cmd RpcCommand) (PendingOperation,error) {
	rs.raft.rpcCh <- cmd

	select {
	case <-ctx.Done():
		var pendingOperation PendingOperation
		return pendingOperation, errors.New(REQUEST_TERMINATED)
	case resp := <-cmd.resp:
		pendingOp, ok := resp.(PendingOperation)
		if !ok {
			panic(ok)
		}

		if !pendingOp.isLeader {
			return pendingOp, errors.New(NOT_LEADER)
		}

		return pendingOp, nil
	}
}

func (rs *RaftRpcServer) Get(ctx context.Context, key *pb.Key) (*pb.Response, error) {
	resp := &pb.Response{}

	if rs.config.Partitioned {
		<-ctx.Done()
		resp.Ok = false
		resp.Response = SIMULATED_PARTITION

		return resp, nil
	}

	op := &pb.Operation{
		Type:  pb.OperationType_GET,
		Key:  key.Key,
	}

	cmd := RpcCommand{
		Command: op,
		resp:    make(chan any, 1),
	}

	pendingOp, err := rs.scheduleRpcCommand(ctx, cmd)
	resp.IsLeader = pendingOp.isLeader
	resp.NewLeader = pendingOp.currentLeader

	if err != nil {
		resp.Ok = false
		resp.Response = err.Error()
		return resp, nil
	}

	res := rs.waitForResult(pendingOp.logIndex, ctx)

	if res.Err != nil {

		resp.Ok = false
		resp.Response = res.Err.Error()
		return resp, nil
	} else {
		resp.Ok = true
		resp.Response = res.Value
		return resp, nil
	}
}

func (rs *RaftRpcServer) Set(ctx context.Context, kvp *pb.KeyValuePair) (*pb.Response, error) {
	resp := &pb.Response{}

	if rs.config.Partitioned {
		<-ctx.Done()
		resp.Ok = false
		resp.Response = SIMULATED_PARTITION

		return resp, nil
	}

	op := &pb.Operation{
		Type:  pb.OperationType_SET,
		Key:   kvp.Key,
		Value: kvp.Value,
	}

	cmd := RpcCommand{
		Command: op,
		resp:    make(chan any, 1),
	}

	pendingOp, err := rs.scheduleRpcCommand(ctx, cmd)
	resp.IsLeader = pendingOp.isLeader
	resp.NewLeader = pendingOp.currentLeader

	if err != nil {
		resp.Ok = false
		resp.Response = err.Error()
		return resp, nil
	}

	rs.waitForResult(pendingOp.logIndex, ctx)
	resp.Ok = true
	return resp, nil
}
func (rs *RaftRpcServer) Delete(ctx context.Context, key *pb.Key) (*pb.Response, error) {
	resp := &pb.Response{}
	if rs.config.Partitioned {
		<-ctx.Done()
		resp.Ok = false
		resp.Response = SIMULATED_PARTITION

		return resp, nil
	}

	op := &pb.Operation{
		Type: pb.OperationType_DELETE,
		Key:  key.Key,
	}

	cmd := RpcCommand{
		Command: op,
		resp:    make(chan any, 1),
	}

	pendingOp, err := rs.scheduleRpcCommand(ctx, cmd)

	resp.IsLeader = pendingOp.isLeader
	resp.NewLeader = pendingOp.currentLeader

	if err != nil {
		resp.Ok = false
		resp.Response = err.Error()
		return resp, nil
	}

	res := rs.waitForResult(pendingOp.logIndex, ctx)
	if res.Err != nil {
		resp.Ok = false
		resp.Response = res.Err.Error()
	} else {
		resp.Ok = true
		resp.Response = res.Value
	}

	return resp, nil
}

func (rs *RaftRpcServer) waitForResult(index int32, ctx context.Context) *KVResult {
	start := time.Now()
	rs.mu.Lock()
	if rs.pendingOps[index] == nil {
		rs.pendingOps[index] = make(chan *KVResult, 1)
	}
	pendingOpsCh := rs.pendingOps[index]
	rs.mu.Unlock()

	select {
	case <-ctx.Done():
		return &KVResult{Err: errors.New("Deadline exceeded")}
	case result := <-pendingOpsCh:
		Infof("Operation %v took %v", rs.raft.log[index].Operation, time.Since(start))
		return result
	}
}

func (rs *RaftRpcServer) Partition(ctx context.Context, in *wrappers.BoolValue) (*empty.Empty, error) {
	rs.config.Partitioned = in.Value
	return &empty.Empty{}, nil
}
