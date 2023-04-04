package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/SuhasHebbar/CS739-P2/config"
	distkvstore "github.com/SuhasHebbar/CS739-P2"
	pb "github.com/SuhasHebbar/CS739-P2/proto"
	"golang.org/x/exp/slog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	slog.Info("Not much going on right now!")
	config := config.GetConfig()

	clients := map[int32]pb.RaftRpcClient{}
	for k, url := range config.Peers {
		opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

		conn, err := grpc.Dial(url, opts...)
		if err != nil {
			slog.Error("Failed to dial", "err", err)
			panic(err)
		}
		defer conn.Close()

		client := pb.NewRaftRpcClient(conn)
		clients[k] = client

	}

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Printf("> ")
		inputLine, _ := reader.ReadString('\n')
		inputLine = strings.Replace(inputLine, "\n", "", -1)
		fmt.Println(inputLine)

		clientIdStr, arguments, _ := strings.Cut(inputLine, " ")
		if arguments == "" {
			fmt.Println("Invalid operation!")
			continue
		}

		clientId, err := strconv.Atoi(clientIdStr)
		if err != nil {
			fmt.Println("Invalid operation! Failed convert string to number")
			continue

		}

		command, arguments, _ := strings.Cut(arguments, " ")
		if arguments == "" {
			fmt.Println("Invalid operation!")
			continue
		}

		command = strings.ToLower(command)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Hour)
		if command == "get" {
			key := pb.Key{Key: arguments}
			response, err := clients[int32(clientId)].Get(ctx, &key)
			if err != nil {
				slog.Debug("err", err)
			}
			slog.Debug("Okay we're past this")

			if err != nil {
				if err.Error() == distkvstore.NON_EXISTENT_KEY_MSG {
					fmt.Println("<Value does not exist>")
				} else {
					fmt.Println(err)
				}
			} else if response.Ok == true {
				fmt.Println(response.Response)
			} else {
				fmt.Println("Someting went wrong!")

			}
		} else if command == "set" {
			key, value, valid := strings.Cut(arguments, " ")
			if valid {
				kvPair := pb.KeyValuePair{Key: key, Value: value}
				_, err := clients[int32(clientId)].Set(ctx, &kvPair)
				if err != nil {
					slog.Debug("err", err)
				}

				fmt.Println("OK")
			}

		} else if command == "delete" {

			key := pb.Key{Key: arguments}
			response, err := clients[int32(clientId)].Delete(ctx, &key)
			if err != nil {
				slog.Debug("err", err)
			}

			if response.Ok == true {
				fmt.Printf("Deleted %v\n", arguments)
			} else if err.Error() == distkvstore.NON_EXISTENT_KEY_MSG {
				fmt.Println("<Value does not exist>")
			} else {
				fmt.Println("Someting went wrong!")

			}
		} else {
			fmt.Println("Invalid operation!")
		}
		cancel()

	}
}
