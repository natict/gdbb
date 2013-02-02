#!/bin/bash

set -o errexit

echo "Creating gdbb database and user, using the following SQL:"
echo '----------------------------------------'
cat mysql/root.sql
echo '----------------------------------------'
if [ -z "$1" ]; then
	echo "Please use mysql's root permissions."
	mysql -uroot -p < mysql/root.sql
else
	mysql -uroot -p"$1" < mysql/root.sql
fi

echo "Creating Procedures..."
mysql -ugdbb gdbb < mysql/init.sql
