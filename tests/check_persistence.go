package main

import (
	"fmt"

	kv "github.com/SuhasHebbar/CS739-P2"
)

func main() {
	logFiles := []string{"raftLog_0", "raftLog_1", "raftLog_2"}
	voteFiles := []string{"raftVote_0", "raftVote_1", "raftVote_2"}
	p := kv.Persistence{}
	for i := 0; i < 3; i++ {
		fmt.Printf("Reading log file for server %v\n", i)
		log, err1 := p.ReadLog(logFiles[i])
		if err1 != nil {
			fmt.Printf("Error while reading log file err: %v\n", err1)
		} else {
			for i, logentry := range log {
				fmt.Printf("Index: %v, Term: %v, Operation: %v\n", i, logentry.Term, logentry.Operation)
			}
		}
		fmt.Printf("Reading vote file for server %v\n", i)
		vote, err2 := p.ReadVote(voteFiles[i])
		if err2 != nil {
			fmt.Printf("Error while reading vote file err: %v\n", err2)
		} else {
			fmt.Printf("CurrentTerm: %v, VotedFor: %v\n", vote.CurrentTerm, vote.VotedFor)
		}
	}
}
