pids=()
for i in $(seq 0 2)
do
	go run -race cmd/server/server.go -id "$i" &
	pid=$!
	pids+=("$pid")
done

for pid in "${pids[@]}"
do
	wait "$pid"
done

