package main

import (
	"context"
	"encoding/gob"
	"errors"
	"reflect"
	"sync"

	"github.com/SuhasHebbar/CS739-P2/common"
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
	pendingOps map[int32]chan any
	mu         sync.Mutex
	pb.UnimplementedRaftRpcServer
	config *common.Config
}

type RpcServer interface {
	GetClient(peerId PeerId) pb.RaftRpcClient
}

const (
	GET    = "GET"
	SET    = "SET"
	DELETE = "DELETE"
)

type Operation struct {
	Name  string
	Key   string
	Value string
}

type PendingOperation struct {
	isLeader bool
	logIndex int32
}

func NewRaftRpcServer(id PeerId, config *common.Config) *RaftRpcServer {
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
	self.pendingOps = map[int32]chan any{}
	self.config = config

	gob.Register(Empty{})
	gob.Register(Operation{})

	go func() {
		self.raft.startServerLoop()
	}()

	go self.startCommitListerLoop()
	return self
}

func (rs *RaftRpcServer) startCommitListerLoop() {
	for {
		op := <-rs.raft.commitCh
		Debugf("Received operation for index %v", op.Index)
		kvop, ok := op.Operation.(Operation)
		if !ok {
			Debugf("Committed operation of wrong type. Actual type is %v", reflect.TypeOf(op.Operation))
			panic("Trouble!")
		}

		var result any
		if kvop.Name == GET {
			value, err := rs.kv.Get(kvop.Key)
			if err != nil {
				result = err
			} else {
				result = value
			}
		} else if kvop.Name == SET {
			rs.kv.Set(kvop.Key, kvop.Value)
			result = nil

		} else {
			err := rs.kv.Delete(kvop.Key)
			result = err
		}

		rs.mu.Lock()
		// rs.raft.Debug("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA Operation is now saved!")
		if rs.pendingOps[op.Index] == nil {
			rs.pendingOps[op.Index] = make(chan any, 1)
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

func (rs *RaftRpcServer) scheduleRpcCommand(ctx context.Context, cmd RpcCommand) (int32, error) {
	rs.raft.rpcCh <- cmd

	var logIndex int32

	select {
	case <-ctx.Done():
		return -1, errors.New(REQUEST_TERMINATED)
	case resp := <-cmd.resp:
		pendingOp, ok := resp.(PendingOperation)
		if !ok {
			panic(ok)
		}
		if !pendingOp.isLeader {
			return -1, errors.New(NOT_LEADER)
		}

		logIndex = pendingOp.logIndex
	}

	return logIndex, nil
}

func (rs *RaftRpcServer) Get(ctx context.Context, key *pb.Key) (*pb.Response, error) {
	if rs.config.Partitioned {
		<-ctx.Done()
		return nil, errors.New(SIMULATED_PARTITION)

	}

	op := Operation{
		Name: GET,
		Key:  key.Key,
	}

	cmd := RpcCommand{
		Command: op,
		resp:    make(chan any, 1),
	}

	logIndex, err := rs.scheduleRpcCommand(ctx, cmd)
	if err != nil {
		return nil, err
	}

	res := rs.waitForResult(logIndex, ctx)

	resp := &pb.Response{}

	switch v := res.(type) {
	case error:
		resp.Ok = false
		resp.Response = v.Error()
		return resp, nil
	case string:
		resp.Ok = true
		resp.Response = v
		return resp, nil
	}

	rs.raft.Debug("Unreachable area reached")
	panic(res)
}
func (rs *RaftRpcServer) Set(ctx context.Context, kvp *pb.KeyValuePair) (*pb.Response, error) {
	if rs.config.Partitioned {
		<-ctx.Done()
		return nil, errors.New(SIMULATED_PARTITION)

	}

	op := Operation{
		Name:  SET,
		Key:   kvp.Key,
		Value: kvp.Value,
	}

	cmd := RpcCommand{
		Command: op,
		resp:    make(chan any, 1),
	}

	logIndex, err := rs.scheduleRpcCommand(ctx, cmd)
	if err != nil {
		return nil, err
	}

	rs.waitForResult(logIndex, ctx)
	return &pb.Response{Ok: true}, nil
}
func (rs *RaftRpcServer) Delete(ctx context.Context, key *pb.Key) (*pb.Response, error) {
	if rs.config.Partitioned {
		<-ctx.Done()
		return nil, errors.New(SIMULATED_PARTITION)

	}

	op := Operation{
		Name: DELETE,
		Key:  key.Key,
	}

	cmd := RpcCommand{
		Command: op,
		resp:    make(chan any, 1),
	}

	logIndex, err := rs.scheduleRpcCommand(ctx, cmd)
	if err != nil {
		return nil, err
	}

	res := rs.waitForResult(logIndex, ctx)
	resp := &pb.Response{Ok: false}
	if res == nil {
		resp.Ok = true
	} else {
		res, ok := res.(error)
		if ok {
			resp.Response = res.Error()
		}
	}
	return resp, nil
}

func (rs *RaftRpcServer) waitForResult(index int32, ctx context.Context) any {
	rs.mu.Lock()
	if rs.pendingOps[index] == nil {
		rs.pendingOps[index] = make(chan any, 1)
	}
	pendingOpsCh := rs.pendingOps[index]
	rs.mu.Unlock()

	select {
	case <-ctx.Done():
		return errors.New("Deadline exceeded")
	case result := <-pendingOpsCh:
		return result
	}
}

func (rs *RaftRpcServer) Partition(ctx context.Context, in *wrappers.BoolValue) (*empty.Empty, error) {
	rs.config.Partitioned = in.Value
	return &empty.Empty{}, nil
}
