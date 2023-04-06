package bench

import (
    "context"
    "strings"
    "math/rand"

    flag "github.com/spf13/pflag"
)

func main() {
    // Initialize random seed
    seed := int64(0xD)
    prng := rand.New(rand.NewSource(seed))

    // parse flags
    confname := flag.String("conf", "bench_config", "Configuration file for the benchmark")
    flag.Parse()

    // extract config options
    config := GetConfig(*confname)

    // construct the raft client
    client := NewClient(config, prng)

    // populate the database with some initial values before running our workloads
    if config.PopulateAllKeys {
        client.PopulateDB(config.ValLen, context.Background())
    }

    // run the workload
    switch strings.ToLower(config.Mode) {
        case RANDOM:
            client.RunRandomWorkload(config.WriteProp, config.ValLen, context.Background())
        case READ_RECENT:
            client.RunReadRecentWorkload(config.WriteProp, config.ValLen, context.Background())
        case READ_MODIFY_UPDATE:
            client.RunReadModifyUpdateWorkload(config.WriteProp, config.ValLen, context.Background())
        case READ_RANGE:
            client.RunReadRangeWorkload(config.WriteProp, config.ValLen, config.RangeScanNumKeys, context.Background())
    }
}
