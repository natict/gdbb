gdbb
====
(Graph DB benchmarks)

Benchmark graph link-prediction algorithms overs different DBMS

The Link Prediction Problem for Social Networks:
http://www.cs.cornell.edu/home/kleinber/link-pred.pdf

Graphs are extracted from the DBLP Computer Science Bibliography in its XML version,
which can be found here: http://dblp.uni-trier.de/xml/

Prerequisites
=============
* axel (to download DBLP, can be replaced with wget)
* bash
* python2.7
* python-lxml
* python-redis (https://github.com/andymccurdy/redis-py)
* mysql-server
* mysql-client
* redis-server (http://redis.io/topics/quickstart)

Quick Start
===========
* Fetch code:

```
git clone https://github.com/natict/gdbb.git
cd gdbb
```

* Generate graphs:

```
chmod u+x ./mkgraphs.sh
./mkgraphs.sh
```

* Create MySQL tables and procedures (you'll need mysql root):

```
chmod u+x ./init-mysql.sh
./init-mysql.sh
```

* Execute MySQL benchmark:

```
chmod u+x ./benchmark-mysql.sh
./benchmark-mysql.sh
```

* Execute Redis benchmark:

```
chmod u+x ./benchmark-redis.py
./benchmark-redis.py
```

Notes
=====
* Graph generation is done in-memory, make sure you have enough
* Some MySQL configuration changes are recommended (significantly speedup load time, allow recursive procedures):

```
innodb_additional_mem_pool_size = 512M
innodb_buffer_pool_size = 512M
innodb_log_file_size = 256M
innodb_log_buffer_size = 256M
max_sp_recursion_depth=10
```

* Using the latest versions of redis-server and redis-py significantly improves performance