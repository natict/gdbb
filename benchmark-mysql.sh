#!/bin/bash

set -o errexit

source ./commons.sh
source ./graphs-conf.sh

run_foo() {
	local gn="$1" 	# graph name
	local fn="$2"	# function name
	local desc="$3"	# description
	local out="$4"	# output file

	t=$(timer)
	echo -e "\t${desc} (${gn})..."
	if [ -z "$out" ]; then
		echo "CALL ${fn}('${gn}');" | mysql -ugdbb gdbb
	else
		echo "CALL ${fn}('${gn}');" | mysql -ugdbb gdbb 2>&1 >$out
	fi
	printf 'Elapsed time: %s\n' $(timer $t)
}

echo "Running benchmarks:"
for g in ${GRAPH_NAMES[@]}; do
	run_foo "$g" "create_cn_table" "Creating common neighbors table"
	run_foo "$g" "create_cnc_table" "Creating common neighbors count table"
	run_foo "$g" "create_neighbors_table" "Creating neighbors table"
	run_foo "$g" "b_Common_Neighbors" "Benchmarking Common Neighbors" "${g}/Common_Neighbors.out"
	run_foo "$g" "b_Jaccard_Coefficient" "Benchmarking Jaccard's Coefficient" "${g}/Jaccard_Coefficient.out"
	run_foo "$g" "b_Adamic_Adar" "Benchmarking Adamic/Adar" "${g}/Adamic_Adar.out"
	run_foo "$g" "b_Preferential_attachment" "Benchmarking Preferential attachment" "${g}/Preferential_attachment.out"
done
