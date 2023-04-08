package kv

import (
	"flag"
	"net"
	"os"

	pb "github.com/SuhasHebbar/CS739-P2/proto"
	"golang.org/x/exp/slog"
	"google.golang.org/grpc"
)

func ServerEntryPoint() {
	opts := slog.HandlerOptions{
		Level: slog.LevelInfo,
	}

	textHandler := opts.NewTextHandler(os.Stdout)
	logger := slog.New(textHandler)
	slog.SetDefault(logger)

	Debugf("this should print %d", 22)
	idArg := flag.Int("id", 0, "The address the server listens on in the format addr:port.")

	flag.Parse()

	id := int32(*idArg)

	config := GetConfig()

	lis, err := net.Listen("tcp", config.Peers[id])

	if err != nil {
		slog.Error("Failed to listen on socket", "err", err)
	}

	grpcServer := grpc.NewServer()

	raftRpc := NewRaftRpcServer(id, &config)

	pb.RegisterRaftRpcServer(grpcServer, raftRpc)
	grpcServer.Serve(lis)
}
