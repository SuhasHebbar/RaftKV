package kv

import (
	"context"
	"fmt"
	"strings"
	"time"

	pb "github.com/SuhasHebbar/CS739-P2/proto"
	"golang.org/x/exp/slog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type TestClient struct {
	LeaderId int
	Clients  map[int32]pb.RaftRpcClient
	Config   *Config
	deadline time.Duration
}

func NewTestClient() *TestClient {

	c := TestClient{Clients: map[int32]pb.RaftRpcClient{}}
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

func (c *TestClient) HandleGet(keystr string, skipQuorum bool, partitionedIds []int, isPartitionedServer bool) {
	key := pb.Key{Key: keystr}
	var response *pb.Response
	var err error
	for i := 0; i < len(c.Config.Peers); i++ {
		ctx, cancel := context.WithTimeout(context.Background(), c.deadline)
		defer cancel()

		clientId := (c.LeaderId + i) % len(c.Config.Peers)
		fmt.Println("Trying leaderId", clientId)
		if skipQuorum {
			response, err = c.Clients[int32(clientId)].FastGet(ctx, &key)
		} else {
			response, err = c.Clients[int32(clientId)].Get(ctx, &key)
		}
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

func (c *TestClient) HandleSet(arguments string, partitionedIds []int, isPartitionedServer bool) {
	key, value, valid := strings.Cut(arguments, " ")
	if !valid {
		fmt.Println("Something went wrong")
		return
	}

	kvPair := pb.KeyValuePair{Key: key, Value: value}

	var response *pb.Response
	var err error
	for i := 0; i < len(c.Config.Peers); i++ {
		ctx, cancel := context.WithTimeout(context.Background(), c.deadline)
		defer cancel()

		clientId := (c.LeaderId + i) % len(c.Config.Peers)
		fmt.Println("Trying leaderId", clientId)
		response, err = c.Clients[int32(clientId)].Set(ctx, &kvPair)
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

	if err != nil || (response != nil && !response.IsLeader) {
		slog.Debug("err %v", err)
		return
	}

	fmt.Println("OK")

}

func (c *TestClient) HandleDelete(arguments string, partitionedIds []int) {
	key := pb.Key{Key: arguments}

	var response *pb.Response
	var err error
	for i := 0; i < len(c.Config.Peers); i++ {
		ctx, cancel := context.WithTimeout(context.Background(), c.deadline)
		defer cancel()

		clientId := (c.LeaderId + i) % len(c.Config.Peers)
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
