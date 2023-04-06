package main

import (
    "encoding/json"
    "io/ioutil"
    "os"
)

type Config struct {
    Peers map[PeerId]string `json:"peers"`
}

func GetConfig() Config {
    jsonFile, err := os.Open("./conf.json")
    if err != nil {
        Debugf("Failed to read config. %v", err)
        panic(err)
    }
    Debugf("Successfully read config file.")

    defer jsonFile.Close()

    fileBytes, _ := ioutil.ReadAll(jsonFile)

    var config Config

    err = json.Unmarshal(fileBytes, &config)
    if err != nil {
        Debugf("Failed to unmarshall config. %v", err)
        panic(err)
    }

    return config
}
