package kv

import (
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
	StoredLogs  *pb.StoredLog
}

func (p *Persistence) WriteLog(filename string) {
	tmpfile, err := ioutil.TempFile("/tmp", "storedlog-*.txt")
	if err != nil {
		Debugf("tmpfile %v", err)
		panic(err)
	}

	tmpfileName := tmpfile.Name()



	buf, err := proto.Marshal(p.StoredLogs)
	if err != nil {
		Debugf("protomarshall %v", err)
		panic(err)
	}


	Infof("Persisting %v log bytes", len(buf))

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

func (p *Persistence) ReadLog(filename string) (*pb.StoredLog, error) {
	storedLog := &pb.StoredLog{}
	buf, err := os.ReadFile(filename)

	if err != nil {
		return storedLog, err
	}

	err = proto.Unmarshal(buf, storedLog)
	if err != nil {
		Debugf("unmarshall %v", err)
		panic(err)
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
