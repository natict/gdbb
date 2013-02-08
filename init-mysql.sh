#!/bin/bash

set -o errexit

# $0 <data-set-dir> [<mysql root password>]
[ ! -d $1 ] && (echo "usage: $0 <data-set-dir>"; exit 1)

echo "Creating gdbb database and user, using the following SQL:"
echo '----------------------------------------'
cat mysql/root.sql
echo '----------------------------------------'
if [ -z "$2" ]; then
	echo "Please use mysql's root permissions."
	mysql -uroot -p < mysql/root.sql
else
	mysql -uroot -p"$2" < mysql/root.sql
fi

echo "Creating Procedures..."
mysql -ugdbb gdbb < mysql/init.sql

echo "Creating Tables..."
MYSQL_CMD="mysql --local-infile=1 -ugdbb gdbb"
# Drop all tables
mysqldump -ugdbb --add-drop-table --no-data gdbb | grep ^DROP | $MYSQL_CMD
# Create nodes and edges tables
echo "CALL create_graph_tables()" | $MYSQL_CMD
# Load csv files
echo "LOAD DATA LOCAL INFILE '${1}/nodes.csv' INTO TABLE nodes FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';" | $MYSQL_CMD
echo "LOAD DATA LOCAL INFILE '${1}/edges.csv' INTO TABLE edges FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';" | $MYSQL_CMD
