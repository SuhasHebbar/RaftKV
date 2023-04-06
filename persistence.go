package kv

import (
	"encoding/gob"
	"fmt"
	"os"
)

type Vote struct {
	CurrentTerm int32
	VotedFor    PeerId
	//Checksum    uint32
}

type LogEntryWCS struct {
	LogEntry
	//Checksum uint32
}

type Persistence struct {
	vote Vote
	log  []LogEntry
}

func (p *Persistence) WriteLog(filename string) error {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0777)
	if err != nil {
		//Debugf("Error is in openfile")
		return err
	}
	defer file.Close()
	enc := gob.NewEncoder(file)

	for _, logEntry := range p.log {
		err = enc.Encode(logEntry)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Persistence) WriteVote(filename string) error {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0777)
	if err != nil {
		return err
	}
	defer file.Close()
	enc := gob.NewEncoder(file)
	err = enc.Encode(p.vote)
	return err
}

func (p *Persistence) ReadLog(filename string) ([]LogEntry, error) {
	file, err := os.Open(filename)
	if err != nil {
		return []LogEntry{}, err
	}
	defer file.Close()
	gob.Register(Empty{})
	gob.Register(Operation{})
	log := []LogEntry{}
	dec := gob.NewDecoder(file)
	var logEntry LogEntry
	for {
		err = dec.Decode(&logEntry)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return []LogEntry{}, err
		}
		log = append(log, LogEntry{Term: logEntry.Term, Operation: logEntry.Operation})
		fmt.Printf("log entry %v\n", logEntry)
	}
	return log, nil
}

func (p *Persistence) ReadVote(filename string) (Vote, error) {
	gob.Register(Empty{})
	gob.Register(Operation{})
	file, err := os.Open(filename)
	if err != nil {
		return Vote{}, err
	}
	defer file.Close()
	vote := Vote{}
	dec := gob.NewDecoder(file)
	err = dec.Decode(&vote)
	if err != nil {
		return Vote{}, err
	}
	return vote, err
}
