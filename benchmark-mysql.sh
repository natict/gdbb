#!/bin/bash

set -o errexit

source ./graphs-conf.sh

echo "Running benchmarks:"
for g in ${GRAPH_NAMES[@]}; do
	echo -e "\tCommon Neighbors ($g)..."
	time echo "CALL common_neighbors('$g');" | mysql -ugdbb gdbb 2>&1 >/dev/null
done
