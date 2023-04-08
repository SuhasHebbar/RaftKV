package kv

import (
	"flag"
	// "fmt"
	"net"
	"os"
	// "os/signal"
	// "syscall"
	"time"

	pb "github.com/SuhasHebbar/CS739-P2/proto"
	"golang.org/x/exp/slog"
	"google.golang.org/grpc"
)

func ServerEntryPoint() {
	// sigs := make(chan os.Signal, 1)
	// signal.Notify(sigs, syscall.SIGTERM)
	//
	// go func() {
	// 	<- sigs
	// 	panic(fmt.Sprintln("timestamp", time.Now().UnixMilli()))
	// }()

	opts := slog.HandlerOptions{
		Level: slog.LevelInfo,
	}

	textHandler := opts.NewTextHandler(os.Stdout)
	logger := slog.New(textHandler)
	slog.SetDefault(logger)

	Infof("Start timestamp: %v", time.Now().UnixMilli())

	// Debugf("this should print %d", 22)
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
