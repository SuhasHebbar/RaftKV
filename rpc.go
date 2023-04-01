package main

import (
	pb "github.com/SuhasHebbar/CS739-P2/proto"

)
type RaftRpcServer struct {
	raft *Raft
	pb.UnimplementedRaftRpcServer
}

type RpcServer interface {
	GetClient(peerId PeerId) pb.RaftRpcClient
}
