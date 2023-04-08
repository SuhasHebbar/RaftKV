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
const UNAVAILABLE_READ_LEASE = "Leader read lease is unavailable"

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
	isLeader      bool
	currentLeader PeerId
	logIndex      int32
	allowFastPath bool
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
	Err   error
}

func (rs *RaftRpcServer) startCommitListerLoop() {
	for {
		op := <-rs.raft.commitCh
		kvop := op.Operation

		rs.raft.Debug("Committing and applying operation. index: %v, operation: %v", op.Index, op.Operation)

		rs.mu.Lock()
		result := &KVResult{}
		if kvop.Type == pb.OperationType_GET {
			value, err := rs.kv.Get(kvop.Key)
			result.Value = value
			result.Err = err
		} else if kvop.Type == pb.OperationType_SET {
			rs.kv.Set(kvop.Key, kvop.Value)
		} else if kvop.Type == pb.OperationType_DELETE {
			err := rs.kv.Delete(kvop.Key)
			result.Err = err
		} else {
			panic("Invalid operation passed to commit listener loop")
		}

		if rs.pendingOps[op.Index] != nil {
			rs.pendingOps[op.Index] <- result
		}
		rs.mu.Unlock()
		// Infof("Returning response for %v", op.Operation)

		if rs.raft.p.InitialLogSize == int(op.Index) {
			Infof("Startup complete %v", time.Now().UnixMilli())
		}
	}

}

func (rs *RaftRpcServer) GetClient(peerId PeerId) pb.RaftRpcClient {
	if rs.config.Partitioned {
		return nil
	}
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

func (rs *RaftRpcServer) scheduleRpcCommand(ctx context.Context, cmd RpcCommand) (PendingOperation, error) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

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

		if pendingOp.logIndex >= 0 {
			rs.pendingOps[pendingOp.logIndex] = make(chan *KVResult, 1)
		}

		return pendingOp, nil
	}
}

func (rs *RaftRpcServer) FastGet(ctx context.Context, key *pb.Key) (*pb.Response, error) {
	resp := &pb.Response{}

	if rs.config.Partitioned {
		<-ctx.Done()
		resp.Ok = false
		resp.Response = SIMULATED_PARTITION

		return resp, nil
	}

	op := &pb.Operation{
		Type: pb.OperationType_FAST_GET,
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


	if !pendingOp.allowFastPath {
		resp.Ok = false
		resp.Response = UNAVAILABLE_READ_LEASE
	}

	rs.mu.Lock()
	result, err := rs.kv.Get(key.Key)

	if err != nil {
		resp.Ok = false
		resp.Response = err.Error()
	} else {
		resp.Ok = true
		resp.Response = result
	}
	rs.mu.Unlock()

	return resp, nil
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
		Type: pb.OperationType_GET,
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
	pendingOpsCh := rs.pendingOps[index]
	rs.mu.Unlock()

	select {
	case <-ctx.Done():
		rs.mu.Lock()
		delete(rs.pendingOps, index)
		rs.mu.Unlock()

		return &KVResult{Err: errors.New("Deadline exceeded")}
	case result := <-pendingOpsCh:
		rs.mu.Lock()
		delete(rs.pendingOps, index)
		rs.mu.Unlock()

		Infof("Operation %v took %v", rs.raft.log[index].Operation, time.Since(start))
		return result
	}
}

func (rs *RaftRpcServer) Partition(ctx context.Context, in *wrappers.BoolValue) (*empty.Empty, error) {
	rs.config.Partitioned = in.Value
	return &empty.Empty{}, nil
}

