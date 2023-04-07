#!/usr/bin/env python

import numpy as np
import json

def main():
	read_latencies = []
	read_throughput = {}
	write_latencies = []
	write_throughput = {}

	with open('/tmp/loglat') as f:
		for line in f:
			json_entry = json.loads(line.strip())
			lat = json_entry["latency"]
			ts = json_entry["timestamp"]
			op = json_entry["operation"]
			if op == "GET":
				read_latencies.append(lat)
				read_throughput[ts] = read_throughput.get(ts, 0) + 1
			else:
				write_latencies.append(lat)
				write_throughput[ts] = write_throughput.get(ts, 0) + 1

	print("median read throughput", np.median(np.array([t for t in read_throughput.values()])))
	print("median write throughput", np.median(np.array([t for t in write_throughput.values()])))
	
	nprl = np.array(read_latencies)
	print("read latencies", np.percentile(nprl, [50, 75, 90, 99]))
	
	npwl = np.array(write_latencies)
	print("read latencies", np.percentile(npwl, [50, 75, 90, 99]))

main()
