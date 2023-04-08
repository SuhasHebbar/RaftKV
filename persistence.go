package kv

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"io/ioutil"
	"os"

	pb "github.com/SuhasHebbar/CS739-P2/proto"
	"github.com/golang/protobuf/proto"
)

type Vote struct {
	CurrentTerm int32
	VotedFor    PeerId
}

type Persistence struct {
	StoredVote *pb.StoredVote
	StoredLogs *pb.StoredLog
}

func (p *Persistence) WriteLogToHandle(file *os.File, logs []*pb.LogEntry) int {
	totalSizePersisted := 0

	for _, log := range logs {
		buf, err := proto.Marshal(log)
		if err != nil {
			Debugf("protomarshall %v", err)
			panic(err)
		}

		size := len(buf)
		sizeBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(sizeBuf, uint32(size))

		h := sha256.New()

		_, err = h.Write(buf)
		if err != nil {
			Debugf("protohash %v", err)
			panic(err)
		}

		hash := h.Sum(nil)
		hash = hash[:4]

		// Start writing log entry details to file.
		_, err = file.Write(sizeBuf)
		if err != nil {
			Debugf("protohash %v", err)
			panic(err)
		}

		_, err = file.Write(hash)
		if err != nil {
			Debugf("protohash %v", err)
			panic(err)
		}

		_, err = file.Write(buf)
		if err != nil {
			Debugf("protohash %v", err)
			panic(err)
		}

		totalSizePersisted += 4*2 + size
	}

	return totalSizePersisted
}

func (p *Persistence) WriteLog(filename string) {
	tmpfile, err := os.CreateTemp("/tmp", "storedlog-*.txt")
	if err != nil {
		Debugf("tmpfile %v", err)
		panic(err)
	}

	tmpfileName := tmpfile.Name()

	totalSizePersisted := p.WriteLogToHandle(tmpfile, p.StoredLogs.Logs)

	Debugf("Persisting %v log bytes", totalSizePersisted)
	tmpfile.Close()

	err = os.Rename(tmpfileName, filename)
	if err != nil {
		Debugf("tmpfilewrite %v", err)
		panic(err)
	}
}

func (p *Persistence) WriteVote(filename string) {
	tmpfile, err := ioutil.TempFile("/tmp", "storedvote-*.txt")
	if err != nil {
		Debugf("tmpfile %v", err)
		panic(err)
	}

	tmpfileName := tmpfile.Name()

	buf, err := proto.Marshal(p.StoredVote)
	if err != nil {
		Debugf("protomarshall %v", err)
		panic(err)
	}

	Infof("Persisting %v vote bytes", len(buf))

	_, err = tmpfile.Write(buf)
	if err != nil {
		Debugf("tmpfilewrite %v", err)
		panic(err)
	}

	tmpfile.Close()

	err = os.Rename(tmpfileName, filename)
	if err != nil {
		Debugf("tmpfilewrite %v", err)
		panic(err)
	}
}

func (p *Persistence) AppendLog(filename string, logs []*pb.LogEntry) {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	defer file.Close()

	if err != nil {
		Debugf("openfile fail %v", err)
		panic(err)
	}

	bytesAdded := p.WriteLogToHandle(file, logs)
	Debugf("Appending %v log bytes", bytesAdded)




}

func (p *Persistence) ReadLog(filename string) (*pb.StoredLog, error) {
	storedLog := &pb.StoredLog{Logs: []*pb.LogEntry{}}
	file, err := os.Open(filename)
	if err != nil {
		return storedLog, err
	}

	for i := 0; ; i++ {
		sizeBuf := make([]byte, 4)
		_, err := file.Read(sizeBuf)
		if err == io.EOF {
			break
		}

		if err != nil {
			Debugf("fileread size %v", err)
			panic(err)
		}

		size := int(binary.LittleEndian.Uint32(sizeBuf))

		hash := make([]byte, 4)
		_, err = file.Read(hash)
		if err != nil {
			Debugf("fileread hash %v", err)
			panic(err)
		}

		protoBin := make([]byte, size)
		_, err = file.Read(protoBin)
		if err != nil {
			Debugf("fileread proto %v", err)
			panic(err)
		}

		h := sha256.New()

		_, err = h.Write(protoBin)
		if err != nil {
			Debugf("proto hash match %v", err)
			panic(err)
		}

		calcHash := h.Sum(nil)
		calcHash = hash[:4]

		if !bytes.Equal(hash, calcHash) {
			Debugf("proto hash mismatch at %v %v", i, err)
			break
		}

		logEntry := &pb.LogEntry{}
		err = proto.Unmarshal(protoBin, logEntry)
		if err != nil {
			Debugf("proto unmarshall issue at %v %v", i, err)
			break
		}

		storedLog.Logs = append(storedLog.Logs, logEntry)

	}

	return storedLog, err
}

func (p *Persistence) ReadVote(filename string) (*pb.StoredVote, error) {
	vote := &pb.StoredVote{}
	buf, err := os.ReadFile(filename)

	if err != nil {
		return vote, err
	}

	err = proto.Unmarshal(buf, vote)
	if err != nil {
		Debugf("unmarshall %v", err)
		panic(err)
	}

	return vote, err
}
