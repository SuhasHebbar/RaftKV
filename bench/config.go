package bench

import (
	"github.com/spf13/viper"
)

type Config struct {
	// IP address of all the replicas
	Replicas map[int32]string `mapstructure:"replicas"`

	// Number of keys to generate
	NumKeys int32 `mapstructure:"num_keys"`

	// one of [random, read_recent, read_modify_update, read_range]
	// read range reads 8 entries at once
	Mode string `mapstructure:"mode"`

	// Proportion of write operations
	WriteProp float64 `mapstructure:"write_prop"`

	// Contiguous range of keys to scan
	// Applicable only in mode read_range
	RangeScanNumKeys int32 `mapstructure:"range_scan_num_keys"`
}

func GetConfig(confname string) Config {
	// set viper defaults
	viper.SetDefault("num_keys", 1024)
	viper.SetDefault("mode", "random")
	viper.SetDefault("write_prop", 0.5)
	viper.SetDefault("range_scan_num_keys", 8)

	viper.SetConfigName(confname) // config filename
	viper.SetConfigType("yaml") // yaml config
	viper.AddConfigPath(".") // look for bench_config.yaml in the current working directoy

	// read config options from file
	if err := viper.ReadInConfig(); err != nil {
		panic("Requires a config file!")
	}

	// Unmarshal config into our struct
	var config Config
	viper.Unmarshal(&config)
	return config
}
