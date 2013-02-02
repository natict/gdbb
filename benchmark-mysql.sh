#!/bin/bash

set -o errexit

MYSQL_CMD="mysql --local-infile=1 -ugdbb gdbb"

source ./commons.sh

gdbbExecute() {
	local cmd="$1"
	echo $cmd | $MYSQL_CMD
}

loadCSV() {
	local name="$1"
	local csv_file="$(readlink -f $2)"
	echo -e "\t$name : $(wc -l $csv_file | cut -f1 -d' ')"
	t=$(timer)
	echo -e "\tLoading ${name}:"
	gdbbExecute "LOAD DATA LOCAL INFILE '$csv_file' INTO TABLE $name FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';"
	printf '\t\tElapsed time: %s\n' $(timer $t)
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
	local fn="$1"	# function name
	local desc="$2"	# description
	local count=$3
	local params="${4:-}"
	local tmpfile=$(mktemp)

	echo -e "\t${desc} ($count random nodes):"

	local total=$(timer)
	local random_nodes_sql="select id from nodes order by rand() limit $count;"
	for n in $($MYSQL_CMD -NBe "$random_nodes_sql"); do
		t=$(timer)
		gdbbExecuteProc "$fn" "$desc" "/dev/null" "${n}${params}"
		tdiff $t >> $tmpfile
	done

	printf "\t\tMin time: %s\n" $(tprint $(sort -n $tmpfile | sed -n 1p))
	printf "\t\tMedian time: %s\n" $(tprint $(sort -n $tmpfile | sed -n $((count/2+1))p))
	printf "\t\tMax time: %s\n" $(tprint $(sort -n $tmpfile | sed -n ${count}p))
	printf "\t\tTotal time: %s\n" $(timer $total)
	
	# Cleanup
	rm $tmpfile
}

gdbbPrintRecCount() {
	printf "\t\tRecords: %d\n" $(echo "select count(*) as c from ${1};" | $MYSQL_CMD | tail -n1)
}

if [ -z "$1" ] || [ ! -d "$1" ]; then
	echo "usage: $0 <data-set>"
	exit 1
fi

g="$1" 

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
gdbbPrintRecCount "cn"
gdbbExecuteProcT "create_cnc_table" "Creating common neighbors count table"
gdbbPrintRecCount "cnc"
gdbbExecuteProcT "create_neighbors_table" "Creating neighbors table"
gdbbPrintRecCount "neighbors"
gdbbExecuteProcT "b_Common_Neighbors" "Benchmarking Common Neighbors" "${g}/Common_Neighbors.out"
gdbbExecuteProcT "b_Jaccard_Coefficient" "Benchmarking Jaccard's Coefficient" "${g}/Jaccard_Coefficient.out"
gdbbExecuteProcT "b_Adamic_Adar" "Benchmarking Adamic/Adar" "${g}/Adamic_Adar.out"
gdbbExecuteProcT "b_Preferential_attachment" "Benchmarking Preferential attachment" "${g}/Preferential_attachment.out"

# Execute per-x benchmarks
gdbbExecutePerX "x_Common_Neighbors" "Benchmarking Common Neighbors for specific node" 1000
gdbbExecutePerX "x_Jaccard_Coefficient" "Benchmarking Jaccard's Coefficient for specific node" 1000
gdbbExecutePerX "x_Adamic_Adar" "Benchmarking Adamic/Adar for specific node" 1000
gdbbExecutePerX "x_Preferential_attachment" "Benchmarking Preferential attachment for specific node" 1000
gdbbExecutePerX "x_Graph_Distance" "Benchmarking Graph Distance for specific node" 1000 ",4,100"
gdbbExecutePerX "x_Katz" "Benchmarking Katz (unweighted) for specific node" 1000 ",4,0.1,100"
gdbbExecutePerX "x_RootedPageRank" "Benchmarking Rooted PageRank for specific node" 10
