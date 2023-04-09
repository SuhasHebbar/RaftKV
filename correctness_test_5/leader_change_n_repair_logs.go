package main

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/wrapperspb"

	kv "github.com/SuhasHebbar/CS739-P2"
	pb "github.com/SuhasHebbar/CS739-P2/proto"
)

var c *kv.SimpleClient

func main() {
	c = kv.NewSimpleClient()
	testLeaderChange()
}

func testLeaderChange() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Hour)

	arguments := "1 2"
	fmt.Println("Sending Request: Set ", arguments)
	c.HandleSet(arguments, []int{}, false)
	arguments = "2 3"
	fmt.Println("Sending Request: Set ", arguments)
	c.HandleSet(arguments, []int{}, false)

	// var followers []int
	// for i := 0; i < 3; i++ {
	// 	if i != c.LeaderId {
	// 		followers = append(followers, i)
	// 	}
	// }
	// for _, follower := range followers {
	// 	fmt.Printf("Partitioning follower with Server Id : %v...\n", follower)
	// 	c.Clients[int32(follower)].Partition(ctx, wrapperspb.Bool(true))
	// }

	fmt.Printf("Partitioning the leader with leaderId : %v...\n", c.LeaderId)
	c.Clients[int32(c.LeaderId)].Partition(ctx, wrapperspb.Bool(true))
	partitionedId := c.LeaderId

	arguments = "3 4"
	fmt.Println("Sending Request: Set ", arguments)
	c.HandleSet(arguments, []int{}, true)

	arguments = "4 5"
	fmt.Println("Sending Request: Set ", arguments)
	c.HandleSet(arguments, []int{partitionedId}, false)

	fmt.Printf("Partitioned leader with serverId: %v joins back the cluster...\n", partitionedId)
	c.Clients[int32(partitionedId)].Partition(ctx, wrapperspb.Bool(false))

	time.Sleep(50 * time.Second)
	checkFinalLogs()

	cancel()
}

// func low_timeout_handleGet(keystr string, skipQuorum bool, partitionedIds []int) {
// 	key := pb.Key{Key: keystr}
// 	fmt.Println(keystr)
// 	var response *pb.Response
// 	var err error
// 	for i := 0; i < len(c.Config.Peers); i++ {
// 		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

// 		clientId := (c.LeaderId + i) % len(c.Config.Peers)
// 		for _, partitionedId := range partitionedIds {
// 			if clientId == partitionedId {
// 				ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
// 				break
// 			}
// 		}
// 		defer cancel()
// 		fmt.Println("Trying leaderId", clientId)
// 		if skipQuorum {
// 			response, err = c.Clients[int32(clientId)].FastGet(ctx, &key)
// 		} else {
// 			response, err = c.Clients[int32(clientId)].Get(ctx, &key)
// 		}
// 		kv.Debugf("response %v, err %v", response, err)

// 		if response == nil {
// 			continue
// 		}
// 		if !response.IsLeader {
// 			continue
// 		}

// 		c.LeaderId = clientId
// 		break
// 	}
// 	if err != nil || (response != nil && !response.IsLeader) {
// 		slog.Debug("err %v", err)
// 		return

// 	}

// 	if response.Ok {
// 		fmt.Println(response.Response)
// 	} else {
// 		if response.Response == kv.NON_EXISTENT_KEY_MSG {
// 			fmt.Println("<Value does not exist>")
// 		} else {
// 			fmt.Println(err)
// 		}

// 	}
// }

// func low_timeout_handleSet(arguments string, partitionedIds []int) {
// 	key, value, valid := strings.Cut(arguments, " ")
// 	if !valid {
// 		fmt.Println("Something went wrong")
// 		return
// 	}

// 	kvPair := pb.KeyValuePair{Key: key, Value: value}

// 	var response *pb.Response
// 	var err error

// 	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

// 	clientId := (c.LeaderId) % len(c.Config.Peers)
// 	defer cancel()

// 	fmt.Println("Trying leaderId", clientId)
// 	response, err = c.Clients[int32(clientId)].Set(ctx, &kvPair)
// 	fmt.Printf("response %v, err %v\n", response, err)

// 	c.LeaderId = clientId
// 	break
// 	}

// 	if err != nil || (response != nil && !response.IsLeader) {
// 		slog.Debug("err %v", err)
// 		return
// 	}

// 	fmt.Println("OK")

// }

func checkFinalLogs() {
	logFiles := []string{"raftlogs0", "raftlogs1", "raftlogs2"}
	p := kv.Persistence{StoredVote: &pb.StoredVote{}, StoredLogs: &pb.StoredLog{}}
	for i := 0; i < 3; i++ {
		fmt.Printf("Reading log file for server %v\n", i)
		log, err1 := p.ReadLog(logFiles[i])
		if err1 != nil {
			fmt.Printf("Error while reading log file err: %v\n", err1)
		} else {
			for i, logentry := range log.Logs {
				if logentry.GetOperation().GetType() == pb.OperationType_NOOP {
					continue
				}
				fmt.Printf("Index: %v, Term: %v, Operation: %v\n", i, logentry.Term, logentry.Operation)
			}
		}
	}
}
