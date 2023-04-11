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
	testLeaderPickOnTerm()
}

func testLeaderPickOnTerm() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Hour)
	arguments := "a 2"
	fmt.Println("Sending Request: Set ", arguments)
	c.HandleSet(arguments, []int{}, false)
	arguments = "a"
	fmt.Println("Sending Request: Get ", arguments)
	c.HandleGet(arguments, false, []int{}, false)

	fmt.Printf("Partitioning the leader with leaderId : %v...\n", c.LeaderId)
	c.Clients[int32(c.LeaderId)].Partition(ctx, wrapperspb.Bool(true))
	partitionedId := c.LeaderId
	c.LeaderId++

	time.Sleep(30 * time.Second)

	arguments = "a 4"
	fmt.Println("Sending Request: Set ", arguments)
	c.HandleSet(arguments, []int{}, false)

	fmt.Printf("Printing the current inconsistent log state for the 3 servers...\n")
	checkLogs()

	time.Sleep(30 * time.Second)

	fmt.Printf("Partitioned leader joins back the cluster...\n")
	c.Clients[int32(partitionedId)].Partition(ctx, wrapperspb.Bool(false))

	c.Clients[int32(c.LeaderId)].Partition(ctx, wrapperspb.Bool(true))
	fmt.Printf("Partitioning the new leader with leaderId : %v...\n", c.LeaderId)

	c.LeaderId++

	time.Sleep(40 * time.Second)

	fmt.Printf("Printing the final log state for the 3 servers...\n")
	checkLogs()

	fmt.Printf("Final get request to check kv store is consistent\n")
	arguments = "a"
	fmt.Println("Sending Request: Get ", arguments)
	c.HandleGet(arguments, false, []int{}, false)
	cancel()
}

func checkLogs() {
	logFiles := []string{"raftlogs0", "raftlogs1", "raftlogs2"}
	p := kv.Persistence{StoredVote: &pb.StoredVote{}, StoredLogs: &pb.StoredLog{}}
	for i := 0; i < 3; i++ {
		fmt.Printf("Reading log file for server %v\n", i)
		log, err1 := p.ReadLog(logFiles[i])
		if err1 != nil {
			fmt.Printf("Error while reading log file err: %v\n", err1)
		} else {
			for _, logentry := range log.Logs {
				if logentry.GetOperation().GetType() == pb.OperationType_NOOP {
					continue
				}
				fmt.Printf("Term: %v, Operation: %v\n", logentry.Term, logentry.Operation)
			}
		}
	}
}
