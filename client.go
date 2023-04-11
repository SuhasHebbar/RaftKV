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
	clients  map[int32]pb.RaftRpcClient
	config   *Config
	deadline time.Duration
}

func NewSimpleClient() *SimpleClient {

	c := SimpleClient{Clients: map[int32]pb.RaftRpcClient{}}
	config := GetConfig()
	c.Config = &config
	for k, url := range c.Config.Peers {
		opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

		conn, err := grpc.Dial(url, opts...)
		if err != nil {
			slog.Error("Failed to dial", "err", err)
			panic(err)
		}

		client := pb.NewRaftRpcClient(conn)
		c.Clients[k] = client

	}

	c.deadline = 10 * time.Hour

	return &c
}

func ClientEntryPoint() {
	opts := slog.HandlerOptions{
		Level: slog.LevelDebug,
	}

	textHandler := opts.NewTextHandler(os.Stdout)
	logger := slog.New(textHandler)
	slog.SetDefault(logger)

	// for i := 0; i < 10; i++ {
	// 	ind := i
	// 	go func() {
	// 		c := NewSimpleClient()
	// 		c.handleSet("hello world")
	// 		for {
	// 			start := time.Now()
	// 			c.handleGet("hello", false)
	// 			end := time.Now()
	//
	// 			fmt.Println(ind, " ran for ", end.Sub(start))
	//
	// 			<-time.After(getRandomTimeout(0, 10))
	// 		}
	// 	}()
	// }
	//
	//
	//
	// reader := bufio.NewReader(os.Stdin)
	//
	// reader.ReadString('\n')

	reader := bufio.NewReader(os.Stdin)
	c := NewSimpleClient()
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
			c.HandleGet(arguments, false, []int{}, false)
		} else if command == "fget" {
			c.HandleGet(arguments, true, []int{}, false)
		} else if command == "set" {
			c.HandleSet(arguments, []int{}, false)
		} else if command == "delete" {
			c.handleDelete(arguments, []int{})

		} else {
			fmt.Println("Invalid operation!")
		}

		end := time.Now()

		fmt.Println("Ran for ", end.Sub(start))

	}
}

func (c *SimpleClient) HandleGet(keystr string, skipQuorum bool, partitionedIds []int, isPartitionedServer bool) {
	key := pb.Key{Key: keystr}
	var response *pb.Response
	var err error
	for i := 0; i < len(c.config.Peers); i++ {
		ctx, cancel := context.WithTimeout(context.Background(), c.deadline)
		defer cancel()
		fmt.Println("Trying leaderId", clientId)
		if skipQuorum {
			response, err = c.Clients[int32(clientId)].FastGet(ctx, &key)
		} else {
			response, err = c.Clients[int32(clientId)].Get(ctx, &key)
		}
		fmt.Printf("response %v, err %v\n", response, err)

		if isPartitionedServer {
			c.LeaderId = (c.LeaderId + 1) % len(c.Config.Peers)
			break
		}

		if response == nil {
			continue
		}
		if !response.IsLeader {
			continue
		}

		c.LeaderId = clientId
		break
	}
	if err != nil || (response != nil && !response.IsLeader) {
		slog.Debug("err %v", err)
		return

	}

	if response.Ok {
		//fmt.Println(response.Response)
	} else {
		if response.Response == NON_EXISTENT_KEY_MSG {
			fmt.Println("<Value does not exist>")
		} else {
			fmt.Println(err)
		}

	}
}

func (c *SimpleClient) HandleSet(arguments string, partitionedIds []int, isPartitionedServer bool) {
	key, value, valid := strings.Cut(arguments, " ")
	if !valid {
		fmt.Println("Something went wrong")
		return
	}

	kvPair := pb.KeyValuePair{Key: key, Value: value}

	var response *pb.Response
	var err error
	for i := 0; i < len(c.config.Peers); i++ {
		ctx, cancel := context.WithTimeout(context.Background(), c.deadline)
		defer cancel()

		fmt.Println("Trying leaderId", clientId)
		response, err = c.Clients[int32(clientId)].Set(ctx, &kvPair)
		if isPartitionedServer {
			response.Ok = false
			response.IsLeader = true
			fmt.Printf("response %v, err %v\n", response, "Deadline Exceeded")
		} else {
			fmt.Printf("response %v, err %v\n", response, err)
		}

		if isPartitionedServer {
			c.LeaderId = (c.LeaderId + 1) % len(c.Config.Peers)
			break
		}

		if response == nil {
			continue
		}
		if !response.IsLeader {
			fmt.Println("not leader..")
			continue
		}

		c.LeaderId = clientId
		break
	}

	if err != nil || (response != nil && !response.IsLeader) {
		slog.Debug("err %v", err)
		return
	}

	fmt.Println("OK")

}

func (c *SimpleClient) handleDelete(arguments string, partitionedIds []int) {
	key := pb.Key{Key: arguments}

	var response *pb.Response
	var err error
	for i := 0; i < len(c.config.Peers); i++ {
		ctx, cancel := context.WithTimeout(context.Background(), c.deadline)
		defer cancel()

		clientId := (c.LeaderId + i) % len(c.Config.Peers)

		for _, partitionedId := range partitionedIds {
			if clientId == partitionedId {
				ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
				break
			}
		}

		fmt.Println("Trying leaderId", clientId)
		response, err = c.Clients[int32(c.LeaderId)].Delete(ctx, &key)
		Debugf("response %v, err %v", response, err)

		if response == nil {
			continue
		}
		if !response.IsLeader {
			continue
		}

		c.LeaderId = clientId
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
