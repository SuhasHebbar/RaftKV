package kvstore

import (
	"context"
	"errors"
	"sync"

	pb "github.com/SuhasHebbar/CS739-P2/proto"
)

const NON_EXISTENT_KEY_MSG = "key does not exist."

type KVStore struct {
	store map[string]string
}

func (kv *KVStore) Get(key string) (string, error) {
	val, hasKey := kv.store[key]

	if hasKey {
		return val, nil
	} else {
		return "", errors.New(NON_EXISTENT_KEY_MSG)
	}
}

func (kv *KVStore) Set(key, value string) {
	kv.store[key] = value
}

func (kv *KVStore) Delete(key string) error {
	_, hasKey := kv.store[key]
	if !hasKey {
		return errors.New(NON_EXISTENT_KEY_MSG)
	}
	delete(kv.store, key)
	return nil
}

func NewKVStore() *KVStore {
	return &KVStore{
		store: map[string]string{},
	}
}

type KVRpcServer struct {
	pb.UnimplementedRaftRpcServer
	kv   KVStore
	lock sync.Mutex
}

func NewKVRpcServer() *KVRpcServer {
	return &KVRpcServer{
		kv:   *NewKVStore(),
		lock: sync.Mutex{},
	}
}

func (kvs *KVRpcServer) Get(c context.Context, key *pb.Key) (*pb.GetResponse, error) {
	kvs.lock.Lock()
	defer kvs.lock.Unlock()

	val, err := kvs.kv.Get(key.Key)

	var status pb.BinaryResponse
	if err != nil {
		status = pb.BinaryResponse_FAILURE
	} else {
		status = pb.BinaryResponse_SUCCESS
	}

	response := &pb.GetResponse{Response: val, Status: status}
	return response, nil
}

func (kvs *KVRpcServer) Set(c context.Context, keyValue *pb.KeyValuePair) (*pb.Response, error) {
	kvs.lock.Lock()
	defer kvs.lock.Unlock()

	kvs.kv.Set(keyValue.Key, keyValue.Value)
	response := &pb.Response{
		Status: pb.BinaryResponse_SUCCESS,
	}

	return response, nil
}

// Return true if key existed previously and was removed, else return false.
func (kvs *KVRpcServer) Delete(c context.Context, key *pb.Key) (*pb.Response, error) {
	kvs.lock.Lock()
	defer kvs.lock.Unlock()

	err := kvs.kv.Delete(key.Key)
	status := pb.BinaryResponse_SUCCESS
	if err != nil {
		status = pb.BinaryResponse_FAILURE
	}

	response := &pb.Response{Status: status}
	return response, nil
}
