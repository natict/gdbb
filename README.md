gdbb
====
(Graph DB benchmarks)

Benchmark graph link-prediction algorithms overs different DBMS

Algorithms can be found in [The Link Prediction Problem for Social Networks](http://www.cs.cornell.edu/home/kleinber/link-pred.pdf)

Graphs are extracted from the [DBLP Computer Science Bibliography XML](http://dblp.uni-trier.de/xml/)

Results can be found [here](https://docs.google.com/spreadsheet/ccc?key=0AiFl0Xaks4G-dG9jZnJWWTF1cG5DZ0hjZk52d0JPMVE#gid=1)

Prerequisites
=============
* [axel](https://alioth.debian.org/projects/axel/) (to download DBLP, can be replaced with wget)
* [bash](http://tiswww.case.edu/php/chet/bash/bashtop.html)
* [python 2.7](http://www.python.org/getit/)
* [python-lxml](http://lxml.de/)
* [python-mysqldb](http://mysql-python.sourceforge.net/)
* [python-redis](https://github.com/andymccurdy/redis-py)
* [mysql 5.5](http://dev.mysql.com/downloads/mysql/)
* [redis-server](http://redis.io/topics/quickstart)
* [Neo4J Community 1.9.M03](http://www.neo4j.org/download)
* [Oracle JDK 6](http://www.oracle.com/technetwork/java/javase/downloads/index.html)
* [py2neo](http://py2neo.org/)

Quick Start
===========
* Fetch code:

```
git clone https://github.com/natict/gdbb.git
cd gdbb
```

* Generate graphs:

```
./mkgraphs.sh
```

* Create MySQL tables and procedures (you'll need mysql root), e.g. for all_core3:

```
./init-mysql.sh all_core3
  ```

* Execute MySQL benchmark:

```
./benchmark_mysql.py all_core3
```

* Execute Redis benchmark (i.e. for all_core3):

```
./benchmark_redis.py -d all_core3
```

* Create Neo4J graphs (you'll need to be a sudo-er) (i.e. for all_core3):

```
./init-neo4j.sh all_core3/
```

* Execute Neo4J benchmark:

```
./benchmark_neo4j.py
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
* Currently the Lua scripts are redis-calls optimized and NOT memory optimized
* Some Redis settings are required:

```
lua-time-limit 600000	# 10 Minutes
```

* Let the Neo4J JVM to allocate more heap (conf/neo4j-wrapper.conf)

```
# Initial Java Heap Size (in MB)
wrapper.java.initmemory=1024

# Maximum Java Heap Size (in MB)
wrapper.java.maxmemory=16384
```
