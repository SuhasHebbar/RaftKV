package main

import (
	"fmt"

	kv "github.com/SuhasHebbar/CS739-P2"
	pb "github.com/SuhasHebbar/CS739-P2/proto"
)

func main() {
	logFiles := []string{"raftlogs0", "raftlogs1", "raftlogs2"}
	voteFiles := []string{"raftvotes0", "raftvotes1", "raftvotes2"}
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
		fmt.Printf("Reading vote file for server %v\n", i)
		vote, err2 := p.ReadVote(voteFiles[i])
		if err2 != nil {
			fmt.Printf("Error while reading vote file err: %v\n", err2)
		} else {
			fmt.Printf("CurrentTerm: %v, VotedFor: %v\n", vote.Term, vote.VotedFor)
		}
	}
}
