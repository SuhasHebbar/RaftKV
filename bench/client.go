package bench

import (
    "context"
    "math/rand"
    "time"
    "fmt"
    "sync"

    pb "github.com/SuhasHebbar/CS739-P2/proto"
    "golang.org/x/exp/slog"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
    "go.uber.org/zap"
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

    // zap logger to log metrics
    zlog *zap.Logger

    // mutex for leader id
    leaderIdCh chan int
}

func NewClient(config *Config, prng *rand.Rand, zlog *zap.Logger) *Client {
    slog.Info("Creating new client ...")

    return &Client{
        replicas: ConnectReplicas(config.Replicas),
        keys: GenerateKeys(prng, config.NumKeys, config.KeyLen),
        leaderId: 0,
        prng: prng,
        zlog: zlog,
        leaderIdCh: make(chan int, 10000),
    }
}

func (client *Client) PopulateDB(valLen int32, pctx context.Context) {
    slog.Info("Populating database ...")

    // we do not expect each operation to take more than a second on average
    ctx, cancel := context.WithTimeout(pctx, time.Duration(len(client.keys)) * time.Second)
    defer cancel()

    var wg sync.WaitGroup

    for _, key := range client.keys {
        select {
        case leaderId := <- client.leaderIdCh:
            client.leaderId = leaderId
        default:
        }

        value := RandStringRunes(client.prng, valLen)

        wg.Add(1)
        slog.Info("Runnig goroutine for kv", "key", key, "value", value)

        // execute these in parallel
        go func(key string, leaderId int, ctx context.Context) {
            defer wg.Done()

            client.Set(key, value, leaderId, ctx)
        }(key, client.leaderId, ctx)
    }

    slog.Info("Waiting for goroutines to finish ...")
    wg.Wait()
    slog.Info("Done waiting ...")
}

func ConnectReplicas(replicas []string) []pb.RaftRpcClient {
    slog.Info("Connecting to replicas ...")

    // Connect to each replica
    clients := make([]pb.RaftRpcClient, len(replicas))
    for i, url := range replicas {
        opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

        conn, err := grpc.Dial(url, opts...)
        if err != nil {
            slog.Error("Failed to dial", "err", err)
            panic(err)
        }

        clients[i] = pb.NewRaftRpcClient(conn)
    }

    return clients
}

func GenerateKeys(prng *rand.Rand, numKeys int32, keyLen int32) []string {
    slog.Info("Generating keys ...")

    keys := make([]string, numKeys)
    for i := 0; i < int(numKeys); i++ {
        keys[i] = RandStringRunes(prng, keyLen)
    }
    return keys
}

func (client *Client) getRandomTimeout(minTimeout, maxTimeout int) time.Duration {
    return time.Duration((minTimeout +
        client.prng.Intn(1+maxTimeout-minTimeout)) * int(time.Millisecond),
    )
}

const NUM_THREADS = 10

func (client *Client) RunRandomWorkload(writeProp float32, valLen int32, ctx context.Context) {
    slog.Info("Starting the random workload with", "writeProp", writeProp, "valLen", valLen)

    for i := 0; i < 10; i++ {
        for {
            time.Sleep(client.getRandomTimeout(50, 100))

            // decide whether to read or write
            if client.prng.Float32() <  writeProp {
                // pick a random key
                key := client.keys[client.prng.Intn(len(client.keys))]

                // pick a random value to write
                value := RandStringRunes(client.prng, valLen)

                // leaderId := client.leaderId
                go func(key, value string, leaderId int, ctx context.Context) {
                    // random write
                    start := time.Now()

                    // kv operation
                    client.Set(key, value, leaderId, ctx)

                    // compute latency
                    client.zlog.Info("SET",
                        zap.Int64("latency", time.Since(start).Nanoseconds()),
                        zap.Int64("timestamp", time.Now().Unix()),
                        zap.String("operation", "SET"),
                    )
                }(key, value, client.leaderId, ctx)
            } else {
                // pick a random key
                key := client.keys[client.prng.Intn(len(client.keys))]

                // leaderId := client.leaderId
                go func(key string, leaderId int, ctx context.Context) {
                    // random read
                    start := time.Now()

                    // kv operation
                    client.Get(key, leaderId, ctx)

                    // compute latency
                    client.zlog.Info("GET",
                        zap.Int64("latency", time.Since(start).Nanoseconds()),
                        zap.Int64("timestamp", time.Now().Unix()),
                        zap.String("operation", "GET"),
                    )
                }(key, client.leaderId, ctx)
            }
        }
    }
}
//
// func (client *Client) RunReadRecentWorkload(writeProp float32, valLen int32, ctx context.Context) {
//     slog.Info("Starting the read recent workload with", "writeProp", writeProp, "valLen", valLen)
//
//     // Initalize recent key randomly
//     //   reset with last written key (same as last read key if previous op is not write)
//     recentKey := client.keys[client.prng.Intn(len(client.keys))]
//
//     // mutex for recent key
//     recentKeyCh := make(chan string)
//
//     triggerTimer := time.After(getTriggerTimeout())
//
//     for {
//         select {
//         case <- triggerTimer:
//             // decide whether to read or write
//             if client.prng.Float32() <  writeProp {
//                 // pick a random key
//                 key := client.keys[client.prng.Intn(len(client.keys))]
//
//                 // pick a random value to write
//                 value := RandStringRunes(client.prng, valLen)
//                 
//                 go func(key, value string, leaderId int, ctx context.Context) {
//                     // random write
//                     start := time.Now()
//
//                     // kv operation
//                     // TODO: measure execution time
//                     client.Set(key, value, leaderId, ctx)
//
//                     // compute latency
//                     client.zlog.Info("SET",
//                         zap.Int64("latency", time.Since(start).Nanoseconds()),
//                         zap.Int64("timestamp", time.Now().Unix()),
//                         zap.String("operation", "SET"),
//                     )
//
//                     // reset recent_key to current key
//                     recentKeyCh <- key
//                 }(key, value, client.leaderId, ctx)
//             } else {
//                 go func(key string, leaderId int, ctx context.Context) {
//                     // recent read
//                     start := time.Now()
//
//                     client.Get(key, leaderId, ctx)
//
//                     // compute latency
//                     client.zlog.Info("GET",
//                         zap.Int64("latency", time.Since(start).Nanoseconds()),
//                         zap.Int64("timestamp", time.Now().Unix()),
//                         zap.String("operation", "GET"),
//                     )
//                 }(recentKey, client.leaderId, ctx)
//             }
//
//             triggerTimer = time.After(getTriggerTimeout())
//         case leaderId := <- client.leaderIdCh:
//             client.leaderId = leaderId
//         case key := <- recentKeyCh:
//             recentKey = key
//         }
//     }
// }
//
// func (client *Client) RunReadModifyUpdateWorkload(writeProp float32, valLen int32, ctx context.Context) {
//     slog.Info("Starting the read modify update workload with", "writeProp", writeProp, "valLen", valLen)
//
//     for {
//         // decide whether to read or write
//         if client.prng.Float32() <  writeProp {
//             // pick a random key
//             key := client.keys[client.prng.Intn(len(client.keys))]
//
//             // pick a random value to write
//             value := RandStringRunes(client.prng, valLen)
//             
//             go func(key, value string, ctx context.Context) {
//                 // read modify update
//                 start := time.Now()
//
//                 // read the value in key
//                 client.Get(key, ctx)
//
//                 // update with new value
//                 client.Set(key, value, ctx)
//
//                 // compute latency
//                 client.zlog.Info("SET",
//                     zap.Int64("latency", time.Since(start).Nanoseconds()),
//                     zap.Int64("timestamp", time.Now().Unix()),
//                     zap.String("operation", "SET"),
//                 )
//             }(key, value, ctx)
//         } else {
//             // pick a random key
//             key := client.keys[client.prng.Intn(len(client.keys))]
//             
//             go func(key string, ctx context.Context) {
//                 // random read
//                 start := time.Now()
//
//                 // kv operation
//                 client.Get(key, ctx)
//
//                 // compute latency
//                 client.zlog.Info("GET",
//                     zap.Int64("latency", time.Since(start).Nanoseconds()),
//                     zap.Int64("timestamp", time.Now().Unix()),
//                     zap.String("operation", "GET"),
//                 )
//             }(key, ctx)
// triggerTimer = time.After(getTriggerTimeout())
//         }
//     }
// }
//
// func (client *Client) RunReadRangeWorkload(writeProp float32, valLen int32, rangeScanNumKeys int32, ctx context.Context) {
//     slog.Info("Starting the read range workload with", "writeProp", writeProp, "valLen", valLen)
//
//     for {
//         // decide whether to read or write
//         if client.prng.Float32() <  writeProp {
//             // pick a random key
//             key := client.keys[client.prng.Intn(len(client.keys))]
//
//             // pick a random value to write
//             value := RandStringRunes(client.prng, valLen)
//
//             go func(key, value string, ctx context.Context) {
//                 // random write
//                 start := time.Now()
//                 
//                 // kv operation
//                 client.Set(key, value, ctx)
//
//                 // compute latency
//                 client.zlog.Info("SET",
//                     zap.Int64("latency", time.Since(start).Nanoseconds()),
//                     zap.Int64("timestamp", time.Now().Unix()),
//                     zap.String("operation", "SET"),
//                 )
//             }(key, value, ctx)
//         } else {
//             id := client.prng.Intn(len(client.keys))
//
//             go func(id int, ctx context.Context) {
//                 // scan a contiguous range from a random index
//                 start := time.Now()
//
//                 for i := 0; i < int(rangeScanNumKeys); i++ {
//                     key := client.keys[(id+i)%len(client.keys)]
//
//                     // kv operation
//                     client.Get(key, ctx)
//                 }
//
//                 // compute latency
//                 client.zlog.Info("GET",
//                     zap.Int64("latency", time.Since(start).Nanoseconds()),
//                     zap.Int64("timestamp", time.Now().Unix()),
//                     zap.String("operation", "GET"),
//                 )
//             }(id, ctx)
//         }
// triggerTimer = time.After(getTriggerTimeout())
//     }
// }

func (client *Client) Get(keystr string, leaderId int, ctx context.Context) {
    key := pb.Key{Key: keystr}

    var response *pb.Response
    var err error

    for i := 0; i < len(client.replicas); i++ {
        clientId := (leaderId + i) % len(client.replicas)
        // fmt.Println("Trying leaderId", clientId)
        response, err = client.replicas[int32(clientId)].Get(ctx, &key)
        // Debugf("response %v, err %v", response, err)

        if response == nil {
            continue
        }
        if !response.IsLeader {
            continue
        }

        client.leaderIdCh <- clientId
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

func (client *Client) Set(key  string, value string, leaderId int, ctx context.Context) {
    kvPair := pb.KeyValuePair{Key: key, Value: value}

    var response *pb.Response
    var err error

    for i := 0; i < len(client.replicas); i++ {
        clientId := (leaderId + i) % len(client.replicas)
        // fmt.Println("Trying leaderId", clientId)
        response, err = client.replicas[int32(clientId)].Set(ctx, &kvPair)
        // Debugf("response %v, err %v", response, err)

        if response == nil {
            continue
        }
        if !response.IsLeader {
            continue
        }

        client.leaderIdCh <- clientId
        break
    }

    if err != nil || (response != nil && !response.IsLeader) {
        slog.Debug("err", err)
    }
}

func (client *Client) Delete(keystr string, leaderId int, ctx context.Context) {
    key := pb.Key{Key: keystr}

    var response *pb.Response
    var err error

    for i := 0; i < len(client.replicas); i++ {
        clientId := (leaderId + i) % len(client.replicas)
        // fmt.Println("Trying leaderId", clientId)
        response, err = client.replicas[int32(clientId)].Delete(ctx, &key)
        // Debugf("response %v, err %v", response, err)

        if response == nil {
            continue
        }
        if !response.IsLeader {
            continue
        }

        // not protected by lock but this is a hint anyway
        client.leaderIdCh <- leaderId
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
