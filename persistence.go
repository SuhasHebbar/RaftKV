package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"unsafe"
)

type Vote struct {
	currentTerm int32
	votedFor    PeerId
}

type Persistence struct {
	vote Vote
	log  []LogEntry
}

func (p *Persistence) writeLog(filename string) error {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND, 0777)
	if err != nil {
		return err
	}
	defer file.Close()
	for _, logentry := range p.log {
		var buf bytes.Buffer
		err = binary.Write(&buf, binary.LittleEndian, logentry)
		if err != nil {
			return err
		}
		_, err = file.Write(buf.Bytes())
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Persistence) writeVote(filename string) error {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		return err
	}
	defer file.Close()
	var buf bytes.Buffer
	err = binary.Write(&buf, binary.LittleEndian, p.vote)
	if err != nil {
		return err
	}
	_, err = file.Write(buf.Bytes())
	if err != nil {
		return err
	}
	return nil
}

func (p *Persistence) readLog(filename string) ([]LogEntry, error) {
	file, err := os.Open(filename)
	if err != nil {
		return []LogEntry{}, err
	}
	defer file.Close()
	log := []LogEntry{}
	for {
		data := make([]byte, unsafe.Sizeof(LogEntry{}))
		_, err := file.Read(data)
		if err != nil {
			if err == io.EOF {
				break
			}
			return []LogEntry{}, err
		}
		buffer := bytes.NewBuffer(data)
		var logentry LogEntry
		err = binary.Read(buffer, binary.LittleEndian, &logentry)
		if err != nil {
			return []LogEntry{}, err
		}
		log = append(log, logentry)
	}
	return log, nil
}

func (p *Persistence) readVote(filename string) (Vote, error) {
	file, err := os.Open(filename)
	if err != nil {
		return Vote{}, err
	}
	defer file.Close()
	data := make([]byte, unsafe.Sizeof(Vote{}))
	_, err = file.Read(data)
	if err != nil {
		return Vote{}, err
	}
	buffer := bytes.NewBuffer(data)
	var vote Vote
	err = binary.Read(buffer, binary.LittleEndian, &vote)
	if err != nil {
		return Vote{}, err
	}
	return vote, nil
}
