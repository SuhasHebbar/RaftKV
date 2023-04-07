pids=()
for i in $(seq 0 2)
do
	go run cmd/server/server.go -id "$i" &
	pid=$!
	pids+=("$pid")
done

for pid in "${pids[@]}"
do
	wait "$pid"
done

