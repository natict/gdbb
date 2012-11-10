#!/bin/bash

set -o errexit

echo "Creating gdbb database and user, using the following SQL:"
echo '----------------------------------------'
cat mysql/root.sql
echo '----------------------------------------'
echo "Please use mysql's root permissions."
mysql -uroot -p < mysql/root.sql

echo "Creating Procedures..."
mysql -ugdbb gdbb < mysql/init.sql

echo "Loading graphs:"
for g in all all_core3 2002_2009 2002_2009_core3 2010_2012 2010_2012_core3; do
	echo -e "\tloading $g..."
	n=$(readlink -f $g/nodes.csv)
	e=$(readlink -f $g/edges.csv)
	echo "CALL create_graph_tables('$g');" | mysql -ugdbb gdbb
	echo "LOAD DATA LOCAL INFILE '$n' INTO TABLE ${g}_nodes FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';" \
		| mysql --local-infile=1 -ugdbb gdbb
	echo "LOAD DATA LOCAL INFILE '$e' INTO TABLE ${g}_edges FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';" \
		| mysql --local-infile=1 -ugdbb gdbb
done
