package kv

import (
	"bytes"
	"encoding/gob"
	"io"
	"os"
	"unsafe"
)

type Vote struct {
	CurrentTerm int32
	VotedFor    PeerId
}

type Persistence struct {
	vote Vote
	log  []LogEntry
}

func (p *Persistence) writeLog(filename string) error {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0777)
	if err != nil {
		Debugf("Error is in openfile")
		return err
	}
	defer file.Close()
	for _, logentry := range p.log {
		buf := new(bytes.Buffer)
		enc := gob.NewEncoder(buf)
		//err = binary.Write(&buf, binary.LittleEndian, logentry)
		err = enc.Encode(logentry)
		if err != nil {
			return err
		}
		_, err = file.Write(buf.Bytes())
		if err != nil {
			Debugf("Error is in Write")
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
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	//err = binary.Write(buf, binary.LittleEndian, p.vote)
	err = enc.Encode(p.vote)
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
		buf := bytes.NewBuffer(data)
		dec := gob.NewDecoder(buf)
		var logentry LogEntry
		//err = binary.Read(buf, binary.LittleEndian, &logentry)
		err = dec.Decode(&logentry)
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
		return Vote{CurrentTerm: 0, VotedFor: -1}, err
	}
	defer file.Close()
	data := make([]byte, unsafe.Sizeof(Vote{}))
	_, err = file.Read(data)
	if err != nil {
		return Vote{CurrentTerm: 0, VotedFor: -1}, err
	}
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	var vote Vote
	//err = binary.Read(buf, binary.LittleEndian, &vote)
	err = dec.Decode(&vote)
	if err != nil {
		return Vote{CurrentTerm: 0, VotedFor: -1}, err
	}
	return vote, nil
}
