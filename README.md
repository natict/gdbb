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

Notes
=====
Graph generation is done in-memory.