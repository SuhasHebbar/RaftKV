package kv

import (
    "context"
    "strings"
    "math/rand"
    "os"

    flag "github.com/spf13/pflag"
    "github.com/SuhasHebbar/CS739-P2/bench"
    "golang.org/x/exp/slog"
)

func BenchEntryPoint() {
    // Initialize random seed
    seed := int64(0xD)
    prng := rand.New(rand.NewSource(seed))

    // parse flags
    confname := flag.String("conf", "bench_config", "Configuration file for the benchmark")
    flag.Parse()

    // extract config options
    config := bench.GetConfig(*confname)

    // setup logger
    opts := slog.HandlerOptions{
            Level: slog.LevelDebug,
    }

    textHandler := opts.NewTextHandler(os.Stdout)
    logger := slog.New(textHandler)
    slog.SetDefault(logger)

    // construct the raft client
    client := bench.NewClient(config, prng)

    // populate the database with some initial values before running our workloads
    if config.PopulateAllKeys {
        client.PopulateDB(config.ValLen, context.Background())
    }

    // run the workload
    switch strings.ToLower(config.Mode) {
        case bench.RANDOM:
            client.RunRandomWorkload(config.WriteProp, config.ValLen, context.Background())
        case bench.READ_RECENT:
            client.RunReadRecentWorkload(config.WriteProp, config.ValLen, context.Background())
        case bench.READ_MODIFY_UPDATE:
            client.RunReadModifyUpdateWorkload(config.WriteProp, config.ValLen, context.Background())
        case bench.READ_RANGE:
            client.RunReadRangeWorkload(config.WriteProp, config.ValLen, config.RangeScanNumKeys, context.Background())
    }
}
