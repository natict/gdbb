#!/bin/bash

CSV_DIR="$1"
NODES_FN="nodes.csv"
EDGES_FN="edges.csv"
TARGET_DIR="neo4j"
NEO4J_DIR="/opt/neo4j"
GRAPHDB_DIR="data/graph.db"

JAVA_CP=$(ls $NEO4J_DIR/lib/*jar | tr '\n' ':')

set -o errexit

function usage() {
	echo -e "usage:\n\t$0 <graph directory>"
	exit 1
}

if [ ! -d "$CSV_DIR" ] || 
	[ ! -r "$CSV_DIR/$NODES_FN" ] ||
	[ ! -r "$CSV_DIR/$EDGES_FN" ]; then
	usage
fi

# Remove graph target dir
rm -fr $CSV_DIR/$TARGET_DIR
# Compile and run GraphLoader
javac -cp $JAVA_CP GraphLoader.java
java -cp $JAVA_CP GraphLoader $CSV_DIR/{$NODES_FN,$EDGES_FN,$TARGET_DIR}
# Stop Neo4J service
sudo /etc/init.d/neo4j-service stop
# Replace graph.db dir
sudo rm -rf $NEO4J_DIR/$GRAPHDB_DIR
sudo mv $CSV_DIR/$TARGET_DIR $NEO4J_DIR/$GRAPHDB_DIR
sudo chown -R neo4j:neo4j $NEO4J_DIR/$GRAPHDB_DIR
# Start Neo4J service
sudo /etc/init.d/neo4j-service start
