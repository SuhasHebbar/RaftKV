package kv

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

type SimpleKVRpcServer struct {
	pb.UnimplementedRaftRpcServer
	kv   KVStore
	lock sync.Mutex
}

func NewKVRpcServer() *SimpleKVRpcServer {
	return &SimpleKVRpcServer{
		kv:   *NewKVStore(),
		lock: sync.Mutex{},
	}
}

func (kvs *SimpleKVRpcServer) Get(c context.Context, key *pb.Key) (*pb.Response, error) {
	kvs.lock.Lock()
	defer kvs.lock.Unlock()

	val, err := kvs.kv.Get(key.Key)
	response := &pb.Response{Response: val, Ok: true, IsLeader: true}
	if err != nil {
		response.Ok = false
		response.Response = err.Error()
	}

	return response, nil
}

func (kvs *SimpleKVRpcServer) Set(c context.Context, keyValue *pb.KeyValuePair) (*pb.Response, error) {
	kvs.lock.Lock()
	defer kvs.lock.Unlock()

	kvs.kv.Set(keyValue.Key, keyValue.Value)
	response := &pb.Response{
		Ok: true,
		IsLeader: true,
	}

	return response, nil
}

// Return true if key existed previously and was removed, else return false.
func (kvs *SimpleKVRpcServer) Delete(c context.Context, key *pb.Key) (*pb.Response, error) {
	kvs.lock.Lock()
	defer kvs.lock.Unlock()

	err := kvs.kv.Delete(key.Key)

	response := &pb.Response{Ok: true, IsLeader: true}
	if err != nil {
		response.Ok = false
		response.Response = err.Error()

	}

	return response, nil
}
