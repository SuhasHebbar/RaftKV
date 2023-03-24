package kvstore

import (
	"context"
	pb "github.com/SuhasHebbar/CS739-P2/proto"
)

type KVStore struct {
	pb.UnimplementedRaftRpcServer
	store map[string]string
}

func NewKVStore() *KVStore {
	return &KVStore{
		store: map[string]string{},
	}
}

func (kv *KVStore) Get(c context.Context, key *pb.Key) (*pb.GetResponse, error) {
	val, hasKey := kv.store[key.Key]

	var status pb.BinaryResponse
	if hasKey {
		status = pb.BinaryResponse_SUCCESS
	}else {
		status = pb.BinaryResponse_FAILURE
	}

	response := &pb.GetResponse{Response: val, Status: status}
	return response, nil
}

func (kv *KVStore) Set(c context.Context, keyValue *pb.KeyValuePair) (*pb.Response, error) {
	kv.store[keyValue.Key] = keyValue.Value
	response := &pb.Response{
		Status: pb.BinaryResponse_SUCCESS,
	}

	return response, nil
}

// Return true if key existed previously and was removed, else return false.
func (kv *KVStore) Delete(c context.Context, key *pb.Key) (*pb.Response, error) {
	_, hasKey := kv.store[key.Key]
	status := pb.BinaryResponse_FAILURE
	if hasKey {
		delete(kv.store, key.Key)
		status = pb.BinaryResponse_SUCCESS

	}

	response := &pb.Response{Status: status}
	return response, nil
}
