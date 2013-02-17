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

# Generate DBLP Graphs
for i in $(seq 0 $((${#DBLP_GRAPH_NAMES[@]}-1))); do 
	t=$(timer)
	echo "Generating ${DBLP_GRAPH_NAMES[$i]}:"
	./dblp2graph.py -i $XML_DIR -o ./${DBLP_GRAPH_NAMES[$i]} ${DBLP_GRAPH_OPTS[$i]}
	printf 'Elapsed time: %s\n' $(timer $t)
done

# Generate SNAP Graphs
for i in $(seq 0 $((${#SNAP_GRAPH_NAMES[@]}-1))); do
	mkdir -p ${SNAP_GRAPH_NAMES[$i]}
	pushd ${SNAP_GRAPH_NAMES[$i]}
	wget $SNAP_BASE_URL/${SNAP_GRAPH_NAMES[$i]}.${SNAP_GRAPH_OPTS[$i]}
	if [ ${SNAP_GRAPH_OPTS[$i]} == "txt.gz" ]; then
		gunzip ${SNAP_GRAPH_NAMES[$i]}.${SNAP_GRAPH_OPTS[$i]}
		grep -v ^# ${SNAP_GRAPH_NAMES[$i]}.txt | tr -d '\r' | awk '{if ($1 != $2) {print $1","$2}}' > edges.csv
		rm ${SNAP_GRAPH_NAMES[$i]}.txt
		(for i in $(awk -F, '{print $1;print$2}' edges.csv | sort -n -u); do echo $i,Node $i; done) > nodes.csv
	elif [ ${SNAP_GRAPH_OPTS[$i]} == "tar.gz" ]; then
		tmpdir=$(mktemp -d ./tmpXXXXXX)
		tar -xzf ${SNAP_GRAPH_NAMES[$i]}.${SNAP_GRAPH_OPTS[$i]} -C $tmpdir
		cat $tmpdir/${SNAP_GRAPH_NAMES[$i]}/*.edges | awk '{if ($1 != $2) {print $1","$2}}' > edges.csv
		rm ${SNAP_GRAPH_NAMES[$i]}.${SNAP_GRAPH_OPTS[$i]}
		rm -rf $tmpdir
		(for i in $(seq 0 $(awk -F, '{print $1;print$2}' edges.csv | sort -n -u | tail -n1)); do echo $i,Node $i; done) > nodes.csv
	fi
	popd
done

# Generate random permutation of the nodes file
for s in ${ALL_GRAPH_NAMES[@]}; do
    shuf $s/nodes.csv -o $s/rand_nodes.csv
done

# Print stats
printf "Set Name : Node Count : Edge Count : Node Size : Edge Size\n"
for s in ${ALL_GRAPH_NAMES[@]}; do
	printf "$s : %d : %d : %s : %s\n" \
		$(wc -l ./$s/nodes.csv | cut -f1 -d' ') \
		$(wc -l ./$s/edges.csv | cut -f1 -d' ') \
		$(du -sh ./$s/nodes.csv | cut -f1) \
		$(du -sh ./$s/edges.csv | cut -f1)
done
