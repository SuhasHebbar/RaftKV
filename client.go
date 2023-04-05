package kv

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	pb "github.com/SuhasHebbar/CS739-P2/proto"
	"golang.org/x/exp/slog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var clients = map[int32]pb.RaftRpcClient{}
var leaderId int = 0
var config = GetConfig()

func ClientEntryPoint() {
	opts := slog.HandlerOptions{
		Level: slog.LevelDebug,
	}

	textHandler := opts.NewTextHandler(os.Stdout)
	logger := slog.New(textHandler)
	slog.SetDefault(logger)

	for k, url := range config.Peers {
		opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

		conn, err := grpc.Dial(url, opts...)
		if err != nil {
			slog.Error("Failed to dial", "err", err)
			panic(err)
		}
		defer conn.Close()

		client := pb.NewRaftRpcClient(conn)
		clients[k] = client

	}

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Printf("> ")
		inputLine, _ := reader.ReadString('\n')
		inputLine = strings.Replace(inputLine, "\n", "", -1)
		command, arguments, _ := strings.Cut(inputLine, " ")
		if arguments == "" {
			fmt.Println("Invalid operation!")
			continue
		}

		command = strings.ToLower(command)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Hour)
		if command == "get" {
			handleGet(arguments, ctx)
		} else if command == "set" {
			handleSet(arguments, ctx)
		} else if command == "delete" {
			handleDelete(arguments, ctx)

		} else {
			fmt.Println("Invalid operation!")
		}
		cancel()

	}
}

func handleGet(keystr string, ctx context.Context) {

	key := pb.Key{Key: keystr}
	fmt.Println(keystr)
	var response *pb.Response
	var err error
	for i := 0; i < len(config.Peers); i++ {
		clientId := (leaderId + i) % len(config.Peers)
		fmt.Println("Trying leaderId", clientId)
		response, err = clients[int32(clientId)].Get(ctx, &key)
		Debugf("response %v, err %v", response, err)

		if response == nil {
			continue
		}
		if !response.IsLeader {
			continue
		}

		leaderId = clientId
		break
	}
	if err != nil || (response != nil && !response.IsLeader) {
		slog.Debug("err", err)
		return

	}
	slog.Debug("Okay we're past this")

	if err != nil {
		if err.Error() == NON_EXISTENT_KEY_MSG {
			fmt.Println("<Value does not exist>")
		} else {
			fmt.Println(err)
		}
	} else if response.Ok == true {
		fmt.Println(response.Response)
	} else {
		fmt.Println("Someting went wrong!")

	}

}

func handleSet(arguments string, ctx context.Context) {
	key, value, valid := strings.Cut(arguments, " ")
	if !valid {
		fmt.Println("Something went wrong")
		return
	}

	kvPair := pb.KeyValuePair{Key: key, Value: value}

	var response *pb.Response
	var err error
	for i := 0; i < len(config.Peers); i++ {
		clientId := (leaderId + i) % len(config.Peers)
		fmt.Println("Trying leaderId", clientId)
		response, err = clients[int32(clientId)].Set(ctx, &kvPair)
		Debugf("response %v, err %v", response, err)
		if response == nil {
			continue
		}
		if !response.IsLeader {
			continue
		}

		leaderId = clientId
		break
	}

	if err != nil || (response != nil && !response.IsLeader) {
		slog.Debug("err", err)
		return
	}

	fmt.Println("OK")

}

func handleDelete(arguments string, ctx context.Context) {

	key := pb.Key{Key: arguments}

	var response *pb.Response
	var err error
	for i := 0; i < len(config.Peers); i++ {
		clientId := (leaderId + i) % len(config.Peers)
		fmt.Println("Trying leaderId", clientId)
		response, err = clients[int32(leaderId)].Delete(ctx, &key)

		if response == nil {
			continue
		}
		if !response.IsLeader {
			continue
		}

		leaderId = clientId
		break
	}

	if response != nil && !response.IsLeader {
		slog.Debug("Not leader")
		return
	}

	if response.Ok == true {
		fmt.Printf("Deleted %v\n", arguments)
	} else if err.Error() == NON_EXISTENT_KEY_MSG {
		fmt.Println("<Value does not exist>")
	} else {
		fmt.Println("Someting went wrong!")

	}
}
