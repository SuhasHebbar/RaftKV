package main

import (
	"context"
	"encoding/gob"

	pb "github.com/SuhasHebbar/CS739-P2/proto"
	"golang.org/x/exp/slog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type RaftRpcServer struct {
	raft    *Raft
	clients map[PeerId]pb.RaftRpcClient
	pb.UnimplementedRaftRpcServer
}

type RpcServer interface {
	GetClient(peerId PeerId) pb.RaftRpcClient
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

	gob.Register(Empty{})

	go func() {
		self.raft.startServerLoop()
	}()

	go func() {
		for {
			<-self.raft.commitCh

		}

	}()
	return self
}

func (rs *RaftRpcServer) GetClient(peerId PeerId) pb.RaftRpcClient {
	return rs.clients[peerId]
}

func (rs *RaftRpcServer) RequestVote(ctx context.Context, in *pb.RequestVoteRequest) (*pb.RequestVoteReply, error) {
	cmd := RpcCommand{
		Command: in,
		resp: make(chan any, 1),
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
	cmd := RpcCommand{
		Command: in,
		resp: make(chan any, 1),
	}

	rs.raft.rpcCh <- cmd
	
	resp, ok := (<-cmd.resp).(*pb.AppendEntriesResponse)
	if !ok {
		Debugf("Could not convert to AppendEntriesResponse")
		panic(ok)
	}

	return resp, nil
	return nil, nil
}
