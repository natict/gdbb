#! /bin/bash

set -o errexit

source ./graphs-conf.sh

# MySQL
echo "Benchmarking all data-sets using MySQL:"
sudo /etc/init.d/mysql start
sleep 1
for s in ${ALL_GRAPH_NAMES[@]}; do
	echo "$s"
	./init-mysql.sh $s $1
	./benchmark_mysql.py $s > $s/mysql.json
	sleep 1
done
sudo /etc/init.d/mysql stop
sleep 1

# Redis
echo "Benchmarking all data-sets using Redis:"
sudo redis-server /etc/redis/redis.conf
sleep 1
for s in ${ALL_GRAPH_NAMES[@]}; do
	echo "$s"
	./benchmark_redis.py -d $s > $s/redis.json
	sleep 1
done
redis-cli shutdown
sleep 1

# Neo4J
echo "Benchmarking all data-sets using Neo4J:"
for s in ${ALL_GRAPH_NAMES[@]}; do
	echo "$s"
	./init-neo4j.sh $s
	sleep 1
	./benchmark_neo4j.py $s > $s/neo4j.json
	sleep 1
done
sudo /etc/init.d/neo4j-service stop
