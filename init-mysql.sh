#!/bin/bash

set -o errexit

source ./commons.sh
source ./graphs-conf.sh

echo "Creating gdbb database and user, using the following SQL:"
echo '----------------------------------------'
cat mysql/root.sql
echo '----------------------------------------'
echo "Please use mysql's root permissions."
mysql -uroot -p < mysql/root.sql

echo "Creating Procedures..."
mysql -ugdbb gdbb < mysql/init.sql
