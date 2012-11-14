#! /bin/bash

set -o errexit

source ./commons.sh
source ./graphs-conf.sh

XML_DIR="./xml"
#WGET="wget"
WGET="axel" #speedup downloads

# Fetch DBLP
mkdir -p $XML_DIR
pushd $XML_DIR
if [ ! -r dblp.xml ]; then
	$WGET http://dblp.uni-trier.de/xml/dblp.xml.gz
	gunzip dblp.xml.gz
fi
if [ ! -r dblp.dtd ]; then
	$WGET http://dblp.uni-trier.de/xml/dblp.dtd
fi
popd

# Generate Graphs
for i in $(seq 0 $((${#GRAPH_NAMES[@]}-1))); do 
	t=$(timer)
	echo "Generating ${GRAPH_NAMES[$i]}:"
	./dblp2graph.py -i $XML_DIR -o ./${GRAPH_NAMES[$i]} ${GRAPH_OPTS[$i]}
	printf 'Elapsed time: %s\n' $(timer $t)
done

# Print stats
printf "Set Name : Node Count : Edge Count : Node Size : Edge Size\n"
for s in ${GRAPH_NAMES[@]}; do
	printf "$s : %d : %d : %s : %s\n" \
		$(wc -l ./$s/nodes.csv | cut -f1 -d' ') \
		$(wc -l ./$s/edges.csv | cut -f1 -d' ') \
		$(du -sh ./$s/nodes.csv | cut -f1) \
		$(du -sh ./$s/nodes.csv | cut -f1)
done
