package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	pb "github.com/SuhasHebbar/CS739-P2/proto"
	"golang.org/x/exp/slog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)



func main() {
	slog.Info("Not much going on right now!")

	serverAddress := flag.String("addr", "localhost:8000", "The address the server listens on in the format addr:port. For example localhost: 8000")

	flag.Parse()

	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

	conn, err := grpc.Dial(*serverAddress, opts...)
	if err != nil {
		slog.Error("Failed to dial", "err", err)
		// panic(err)
	}
	defer conn.Close()

	client := pb.NewRaftRpcClient(conn)

	reader := bufio.NewReader(os.Stdin)
	for ;; {
		fmt.Printf("> ")
		inputLine, _ := reader.ReadString('\n')
		inputLine = strings.Replace(inputLine, "\n", "", -1)
		fmt.Println(inputLine)

		command, arguments, _ := strings.Cut(inputLine, " ")
		if arguments == "" {
			fmt.Println("Invalid operation!")
			continue
		}

		command = strings.ToLower(command)
		
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if command == "get" {
			key := pb.Key {Key: arguments}
			response, err := client.Get(ctx, &key)
			if err != nil {
				slog.Error("err", err)
			}

			if response.Status == pb.BinaryResponse_SUCCESS {
				fmt.Println(response.Response)
			} else {
				fmt.Println("<Value does not exist>")
			}
		} else if command == "set" {
			key, value, valid := strings.Cut(arguments, " ")
			if valid {
				kvPair := pb.KeyValuePair{Key: key, Value: value}
				_, err := client.Set(ctx, &kvPair)
				if err != nil {
					slog.Error("err", err)
				}

				fmt.Println("OK")
			}

		} else if command == "delete" {

			key := pb.Key {Key: arguments}
			response, err := client.Delete(ctx, &key)
			if err != nil {
				slog.Error("err", err)
			}

			if response.Status == pb.BinaryResponse_SUCCESS {
				fmt.Printf("Deleted %v\n", arguments)
			} else {
				fmt.Println("<Value does not exist>")
			}
		} else {
			fmt.Println("Invalid operation!")
		}
		cancel()


	}
}
