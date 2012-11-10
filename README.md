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
* python2.7
* python-lxml
* mysql-server

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

* Create MySQL tables (you'll need mysql root):

```
chmod u+x ./init-mysql.sh
./init-mysql.sh
```

Notes
=====
* Graph generation is done in-memory, make sure you have enough
* Some MySQL configuration changes are recommended (significantly speedup load time):

```
innodb_additional_mem_pool_size = 512M
innodb_buffer_pool_size = 512M
innodb_log_file_size = 256M
innodb_log_buffer_size = 256M
```