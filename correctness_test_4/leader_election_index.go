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
	testLeaderPickOnIndex()
}

func testLeaderPickOnIndex() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Hour)
	arguments := "1 2"
	fmt.Println("Sending Request: Set ", arguments)
	c.HandleSet(arguments, []int{}, false)
	arguments = "2 3"
	fmt.Println("Sending Request: Set ", arguments)
	c.HandleSet(arguments, []int{}, false)
	var follower int
	for i := 0; i < 3; i++ {
		if i != c.LeaderId {
			follower = i
			break
		}
	}
	fmt.Printf("Partitioning follower with Server Id : %v...\n", follower)
	c.Clients[int32(follower)].Partition(ctx, wrapperspb.Bool(true))
	arguments = "3 4"
	fmt.Println("Sending Request: Set ", arguments)
	c.HandleSet(arguments, []int{follower}, false)
	time.Sleep(30 * time.Second)

	c.Clients[int32(c.LeaderId)].Partition(ctx, wrapperspb.Bool(true))
	fmt.Printf("Partitioning the leader with leaderId : %v...\n", c.LeaderId)

	c.Clients[int32(follower)].Partition(ctx, wrapperspb.Bool(false))
	fmt.Printf("Partitioned follower with Server Id : %v joins back...\n", follower)

	time.Sleep(50 * time.Second)
	checkFinalLogs()

	cancel()
}

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
