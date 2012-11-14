#!/bin/bash

set -o errexit

source ./commons.sh
source ./graphs-conf.sh

echo "Running benchmarks:"
for g in ${GRAPH_NAMES[@]}; do
	t=$(timer)
	echo -e "\tCommon Neighbors ($g)..."
	echo "CALL common_neighbors('$g');" | mysql -ugdbb gdbb 2>&1 > ${g}/common_neighbors.out
	printf 'Elapsed time: %s\n' $(timer $t)
done
