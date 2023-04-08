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

type SimpleClient struct {
	leaderId int
	clients map[int32]pb.RaftRpcClient
	config *Config

}

func NewSimpleClient() *SimpleClient {

	c := SimpleClient{clients: map[int32]pb.RaftRpcClient{}}
	config := GetConfig()
	c.config = &config
	for k, url := range c.config.Peers {
		opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

		conn, err := grpc.Dial(url, opts...)
		if err != nil {
			slog.Error("Failed to dial", "err", err)
			panic(err)
		}

		client := pb.NewRaftRpcClient(conn)
		c.clients[k] = client

	}

	return &c
}

func ClientEntryPoint() {
	opts := slog.HandlerOptions{
		Level: slog.LevelDebug,
	}

	textHandler := opts.NewTextHandler(os.Stdout)
	logger := slog.New(textHandler)
	slog.SetDefault(logger)

	c := NewSimpleClient()


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


		start := time.Now()
		if command == "get" {
			c.handleGet(arguments, false)
		} else if command == "fget" {
			c.handleGet(arguments, true)
		} else if command == "set" {
			c.handleSet(arguments)
		} else if command == "delete" {
			c.handleDelete(arguments)

		} else {
			fmt.Println("Invalid operation!")
		}

		end:= time.Now()

		fmt.Println("Ran for ", end.Sub(start))

	}
}

func (c *SimpleClient) handleGet(keystr string, skipQuorum bool) {
	key := pb.Key{Key: keystr}
	fmt.Println(keystr)
	var response *pb.Response
	var err error
	for i := 0; i < len(c.config.Peers); i++ {
		// ctx, cancel := context.WithTimeout(context.Background(), 10*time.Hour)
		// defer cancel()
		ctx := context.Background()

		clientId := (c.leaderId + i) % len(c.config.Peers)
		fmt.Println("Trying leaderId", clientId)
		if skipQuorum {
			response, err = c.clients[int32(clientId)].FastGet(ctx, &key)
		} else {
			response, err = c.clients[int32(clientId)].Get(ctx, &key)
		}
		Debugf("response %v, err %v", response, err)

		if response == nil {
			continue
		}
		if !response.IsLeader {
			continue
		}

		c.leaderId = clientId
		break
	}
	if err != nil || (response != nil && !response.IsLeader) {
		slog.Debug("err %v", err)
		return

	}

	if response.Ok {
		fmt.Println(response.Response)
	} else {
		if response.Response == NON_EXISTENT_KEY_MSG {
			fmt.Println("<Value does not exist>")
		} else {
			fmt.Println(err)
		}

	}
}

func (c *SimpleClient) handleSet(arguments string) {
	key, value, valid := strings.Cut(arguments, " ")
	if !valid {
		fmt.Println("Something went wrong")
		return
	}

	kvPair := pb.KeyValuePair{Key: key, Value: value}

	var response *pb.Response
	var err error
	for i := 0; i < len(c.config.Peers); i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Hour)
		defer cancel()

		clientId := (c.leaderId + i) % len(c.config.Peers)
		fmt.Println("Trying leaderId", clientId)
		response, err = c.clients[int32(clientId)].Set(ctx, &kvPair)
		Debugf("response %v, err %v", response, err)
		if response == nil {
			continue
		}
		if !response.IsLeader {
			continue
		}

		c.leaderId = clientId
		break
	}

	if err != nil || (response != nil && !response.IsLeader) {
		slog.Debug("err %v", err)
		return
	}

	fmt.Println("OK")

}

func (c *SimpleClient) handleDelete(arguments string) {
	key := pb.Key{Key: arguments}

	var response *pb.Response
	var err error
	for i := 0; i < len(c.config.Peers); i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Hour)
		defer cancel()

		clientId := (c.leaderId + i) % len(c.config.Peers)
		fmt.Println("Trying leaderId", clientId)
		response, err = c.clients[int32(c.leaderId)].Delete(ctx, &key)
		Debugf("response %v, err %v", response, err)

		if response == nil {
			continue
		}
		if !response.IsLeader {
			continue
		}

		c.leaderId = clientId
		break
	}

	if response != nil && !response.IsLeader {
		slog.Debug("Not leader")
		return
	}

	if response.Ok == true {
		fmt.Printf("Deleted %v\n", arguments)
	} else if response.Response == NON_EXISTENT_KEY_MSG {
		fmt.Println("<Value does not exist>")
	} else {
		fmt.Println("Someting went wrong!")

	}
}
