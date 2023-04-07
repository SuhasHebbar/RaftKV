package bench

import (
    "context"
    "math/rand"
    "time"
    "fmt"

    pb "github.com/SuhasHebbar/CS739-P2/proto"
    "golang.org/x/exp/maps"
    "golang.org/x/exp/slog"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

const NON_EXISTENT_KEY_MSG = "key does not exist."

type Client struct {
    // Connections to each of the replicas forming the kv server
    replicas []pb.RaftRpcClient

    // Represents the complete key domain space
    keys []string

    // Cache leader id since we expect leaders to change infrequenctly
    leaderId int

    // Pseudo random number generator
    prng *rand.Rand
}

func NewClient(config *Config, prng *rand.Rand) *Client {
    return &Client{
        replicas: ConnectReplicas(config.Replicas),
        keys: GenerateKeys(prng, config.NumKeys, config.KeyLen),
        leaderId: 0,
        prng: prng,
    }
}

func (client *Client) PopulateDB(valLen int32, pctx context.Context) {
    // we do not expect each operation to take more than a second on average
    ctx, cancel := context.WithTimeout(pctx, time.Duration(len(client.keys)) * time.Second)
    defer cancel()

    for _, key := range client.keys {
        value := RandStringRunes(client.prng, valLen)
        client.Set(key, value, ctx)
    }
}

func ConnectReplicas(replicas map[int32]string) []pb.RaftRpcClient {
    // Connect to each replica
    clients := map[int32]pb.RaftRpcClient{}
    for k, url := range replicas {
        opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

        conn, err := grpc.Dial(url, opts...)
        if err != nil {
            slog.Error("Failed to dial", "err", err)
            panic(err)
        }

        client := pb.NewRaftRpcClient(conn)
        clients[k] = client
    }

    return maps.Values(clients)
}

func GenerateKeys(prng *rand.Rand, numKeys int32, keyLen int32) []string {
    keys := make([]string, numKeys)
    for i := 0; i < int(numKeys); i++ {
        keys[i] = RandStringRunes(prng, keyLen)
    }
    return keys
}

func (client *Client) RunRandomWorkload(writeProp float32, valLen int32, ctx context.Context) {
    for {
        // decide whether to read or write
        if client.prng.Float32() <  writeProp {
            // random write
            
            // pick a random key
            key := client.keys[client.prng.Intn(len(client.keys))]

            // pick a random value to write
            value := RandStringRunes(client.prng, valLen)

            // kv operation
            // TODO: measure execution time
            client.Set(key, value, ctx)
        } else {
            // random read
            
            // pick a random key
            key := client.keys[client.prng.Intn(len(client.keys))]

            // kv operation
            // TODO: measure execution time
            client.Get(key, ctx)
        }
    }
}

func (client *Client) RunReadRecentWorkload(writeProp float32, valLen int32, ctx context.Context) {
    // Initalize recent key randomly
    //   reset with last written key (same as last read key if previous op is not write)
    recent_key := client.keys[client.prng.Intn(len(client.keys))]

    for {
        // decide whether to read or write
        if client.prng.Float32() <  writeProp {
            // random write
            
            // pick a random key
            key := client.keys[client.prng.Intn(len(client.keys))]

            // pick a random value to write
            value := RandStringRunes(client.prng, valLen)

            // kv operation
            // TODO: measure execution time
            client.Set(key, value, ctx)

            // reset recent_key to current key
            recent_key = key
        } else {
            // recent read
            // TODO: measure execution time
            client.Get(recent_key, ctx)
        }
    }
}

func (client *Client) RunReadModifyUpdateWorkload(writeProp float32, valLen int32, ctx context.Context) {
    for {
        // decide whether to read or write
        if client.prng.Float32() <  writeProp {
            // read modify update
            
            // pick a random key
            key := client.keys[client.prng.Intn(len(client.keys))]

            // read the value in key
            client.Get(key, ctx)

            // pick a random value to write
            value := RandStringRunes(client.prng, valLen)

            // update with new value
            // TODO: measure execution time
            client.Set(key, value, ctx)
        } else {
            // random read
            
            // pick a random key
            key := client.keys[client.prng.Intn(len(client.keys))]

            // kv operation
            // TODO: measure execution time
            client.Get(key, ctx)
        }
    }
}

func (client *Client) RunReadRangeWorkload(writeProp float32, valLen int32, rangeScanNumKeys int32, ctx context.Context) {
    for {
        // decide whether to read or write
        if client.prng.Float32() <  writeProp {
            // random write
            
            // pick a random key
            key := client.keys[client.prng.Intn(len(client.keys))]

            // pick a random value to write
            value := RandStringRunes(client.prng, valLen)

            // kv operation
            // TODO: measure execution time
            client.Set(key, value, ctx)
        } else {
            // scan a contiguous range from a random index
            id := client.prng.Intn(len(client.keys))
            for i := 0; i < int(rangeScanNumKeys); i++ {
                key := client.keys[(id+i)%len(client.keys)]

                // kv operation
                // TODO: measure execution time
                client.Get(key, ctx)
            }
        }
    }
}

func (client *Client) Get(keystr string, ctx context.Context) {
    key := pb.Key{Key: keystr}

    var response *pb.Response
    var err error

    for i := 0; i < len(client.replicas); i++ {
        clientId := (client.leaderId + i) % len(client.replicas)
        fmt.Println("Trying leaderId", clientId)
        response, err = client.replicas[int32(clientId)].Get(ctx, &key)
        Debugf("response %v, err %v", response, err)

        if response == nil {
            continue
        }
        if !response.IsLeader {
            continue
        }

        client.leaderId = clientId
        break
    }

    if err != nil || (response != nil && !response.IsLeader) {
        slog.Debug("err", err)
        return
    }

    if err != nil {
        if err.Error() == NON_EXISTENT_KEY_MSG {
            fmt.Println("<Value does not exist>")
        } else {
            fmt.Println(err)
        }
    } else if response.Ok == true {
        fmt.Println(response.Response)
    } else {
        fmt.Println("Internal error!")
    }
}

func (client *Client) Set(key  string, value string, ctx context.Context) {
    kvPair := pb.KeyValuePair{Key: key, Value: value}

    var response *pb.Response
    var err error

    for i := 0; i < len(client.replicas); i++ {
        clientId := (client.leaderId + i) % len(client.replicas)
        fmt.Println("Trying leaderId", clientId)
        response, err = client.replicas[int32(clientId)].Set(ctx, &kvPair)
        Debugf("response %v, err %v", response, err)

        if response == nil {
            continue
        }
        if !response.IsLeader {
            continue
        }

        client.leaderId = clientId
        break
    }

    if err != nil || (response != nil && !response.IsLeader) {
        slog.Debug("err", err)
    }
}

func (client *Client) Delete(keystr string, ctx context.Context) {
    key := pb.Key{Key: keystr}

    var response *pb.Response
    var err error

    for i := 0; i < len(client.replicas); i++ {
        clientId := (client.leaderId + i) % len(client.replicas)
        fmt.Println("Trying leaderId", clientId)
        response, err = client.replicas[int32(clientId)].Delete(ctx, &key)
        Debugf("response %v, err %v", response, err)

        if response == nil {
            continue
        }
        if !response.IsLeader {
            continue
        }

        client.leaderId = clientId
        break
    }

    if err != nil || (response != nil && !response.IsLeader) {
        slog.Debug("err", err)
        return
    }

    if err != nil {
        if err.Error() == NON_EXISTENT_KEY_MSG {
            fmt.Println("<Value does not exist>")
        } else {
            fmt.Println(err)
        }
    } else if response.Ok == true {
        fmt.Println(response.Response)
    } else {
        fmt.Println("Internal error!")
    }
}
