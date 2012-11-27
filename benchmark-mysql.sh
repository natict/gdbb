#!/bin/bash

set -o errexit

MYSQL_CMD="mysql --local-infile=1 -ugdbb gdbb"

source ./commons.sh
source ./graphs-conf.sh

gdbbExecute() {
	local cmd="$1"
	echo $cmd | $MYSQL_CMD
}

loadCSV() {
	local name="$1"
	local csv_file="$(readlink -f $2)"
	echo -e "\t$name : $(wc -l $csv_file | cut -f1 -d' ')"
	t=$(timer)
	echo -e "\tLoading ${name}..."
	gdbbExecute "LOAD DATA LOCAL INFILE '$csv_file' INTO TABLE $name FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';"
	printf '\tElapsed time: %s\n' $(timer $t)
}

gdbbExecuteProc() {
	local fn="$1"	# function name
	local desc="$2"	# description
	local out="$3"	# output file
	local params="${4:-}"

	if [ -z "$out" ]; then
		echo "CALL ${fn}(${params});" | $MYSQL_CMD
	else
		echo "CALL ${fn}(${params});" | $MYSQL_CMD 2>&1 >$out
	fi
}

gdbbExecuteProcT() {
	t=$(timer)
	echo -e "\t${2}:"
	gdbbExecuteProc "$@"
	printf '\t\tElapsed time: %s\n' $(timer $t)
}

gdbbExecutePerX() {
	local nodes="$1"
	local fn="$2"	# function name
	local desc="$3"	# description
	local params="${4:-}"
	local tmpfile=$(mktemp)

	echo -e "\t${desc} (1000 first nodes):"

	local total=$(timer)
	for n in $(head -n1000 $nodes | cut -f1 -d','); do
		t=$(timer)
		gdbbExecuteProc "$fn" "$desc" "/dev/null" "${n}${params}"
		tdiff $t >> $tmpfile
	done

	printf "\t\tMin time: %s\n" $(tprint $(sort -n $tmpfile | sed -n 1p))
	printf "\t\tMedian time: %s\n" $(tprint $(sort -n $tmpfile | sed -n 501p))
	printf "\t\tMax time: %s\n" $(tprint $(sort -n $tmpfile | sed -n 1000p))
	printf "\t\tTotal time: %s\n" $(timer $total)
	
	# Cleanup
	rm $tmpfile
}

for g in ${GRAPH_NAMES[@]}; do

	echo -e "Benchmarking data-set $g:"

	# Drop all tables
	mysqldump -ugdbb --add-drop-table --no-data gdbb | grep ^DROP | $MYSQL_CMD

	# Create nodes and edges tables
	gdbbExecuteProc "create_graph_tables" "Creating nodes and edges tables"

	# Load csv files
	loadCSV "nodes" "${g}/nodes.csv"
	loadCSV "edges" "${g}/edges.csv"

	# Execute global benchmarks
	gdbbExecuteProcT "create_cn_table" "Creating common neighbors table"
	gdbbExecuteProcT "create_cnc_table" "Creating common neighbors count table"
	gdbbExecuteProcT "create_neighbors_table" "Creating neighbors table"
	gdbbExecuteProcT "b_Common_Neighbors" "Benchmarking Common Neighbors" "${g}/Common_Neighbors.out"
	gdbbExecuteProcT "b_Jaccard_Coefficient" "Benchmarking Jaccard's Coefficient" "${g}/Jaccard_Coefficient.out"
	gdbbExecuteProcT "b_Adamic_Adar" "Benchmarking Adamic/Adar" "${g}/Adamic_Adar.out"
	gdbbExecuteProcT "b_Preferential_attachment" "Benchmarking Preferential attachment" "${g}/Preferential_attachment.out"

	# Execute per-x benchmarks
	gdbbExecutePerX "${g}/nodes.csv" "x_Common_Neighbors" "Benchmarking Common Neighbors for specific node"
	gdbbExecutePerX "${g}/nodes.csv" "x_Jaccard_Coefficient" "Benchmarking Jaccard's Coefficient for specific node"
	gdbbExecutePerX "${g}/nodes.csv" "x_Adamic_Adar" "Benchmarking Adamic/Adar for specific node"
	gdbbExecutePerX "${g}/nodes.csv" "x_Preferential_attachment" "Benchmarking Preferential attachment for specific node"
	gdbbExecutePerX "${g}/nodes.csv" "x_Graph_Distance" "Benchmarking Graph Distance for specific node" ",4,100"
	gdbbExecutePerX "${g}/nodes.csv" "x_Katz" "Benchmarking Katz (unweighted) for specific node" ",4,0.1,100"
done
