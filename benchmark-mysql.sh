#!/bin/bash

set -o errexit

echo "Running benchmarks:"
for g in all all_core3 2002_2009 2002_2009_core3 2010_2012 2010_2012_core3; do
	echo -e "\tCommon Neighbors ($g)..."
	echo "CALL common_neighbors('$g');" | mysql -ugdbb gdbb
done
