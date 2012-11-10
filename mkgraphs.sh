#! /bin/bash

set -o errexit

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
./dblp2graph.py -i $XML_DIR -o ./all &
./dblp2graph.py -i $XML_DIR -o ./2002_2009 -e 2009-12-31 &
./dblp2graph.py -i $XML_DIR -o ./2010_2012 -s 2010-01-01 &

wait

./dblp2graph.py -i $XML_DIR -o ./all_core3 --core 3 &
./dblp2graph.py -i $XML_DIR -o ./2002_2009_core3 -e 2009-12-31 --core 3 &
./dblp2graph.py -i $XML_DIR -o ./2010_2012_core3 -s 2010-01-01 --core 3 &

wait

# Print stats
printf "Set Name\t:\tNode Count\t:\tEdge Count\n"
for s in all all_core3 2002_2009 2002_2009_core3 2010_2012 2010_2012_core3; do
	printf "$s\t:\t%d\t:\t%d\n" \
		$(wc -l ./$s/nodes.csv | cut -f1 -d' ') \
		$(wc -l ./$s/edges.csv | cut -f1 -d' ')
done
