#!/usr/bin/env python2.7

from __future__ import print_function
from random import randint
from py2neo import neo4j, rest, cypher

import time

class benchmark(object):
	def __init__(self, func):
		self.func = func
	def __call__(self, *args):
		t = time.time()
		ret = self.func(*args)
		t = time.time()-t
		if ret is None:
			return t
		else:
			print("DEBUG: time elapsed: %g" % t)
			return ret

pInitNeighbors = """
	START a=node(*)
	WHERE has(a.nid)
	SET a.neighbors=0
"""

pSetNeighbors = """
	START a=node(*)
	MATCH (a)-[:COAUTH]->(n)
	WITH a, count(n) as cn
	SET a.neighbors=cn
"""

pGetTopN = """
	START a=node(*)
	WHERE has(a.neighbors)
	RETURN a
	ORDER BY a.neighbors DESC
	LIMIT 101
"""

pGetNodeCount = """
	START a=node(*) 
	RETURN count(a)
"""

# bCommonNeighbors Cypher perform really bad with multiple start points (In particular, with *)
bCommonNeighbors = '''
	START a=node(*) 
	WHERE a.neighbors!>%d 
	WITH a 
	MATCH (a)-[:COAUTH]->(b)<-[:COAUTH]-(c) 
	WHERE a.nid<c.nid 
	RETURN a.nid,c.nid,count(b) AS cn 
	ORDER BY cn DESC 
	LIMIT 100
'''

xCommonNeighbors = """
	START a=node({n}) 
	MATCH (a)-[:COAUTH]->(b)<-[:COAUTH]-(c) 
	WHERE a <> c 
	RETURN a.nid,c.nid,COUNT(b) as score
	ORDER BY score DESC 
	LIMIT 100
"""

xJaccardsCoefficient = """
	START a=node({n}) 
	MATCH (a)-[:COAUTH]->(b)<-[:COAUTH]-(c)
	WHERE a <> c 
	WITH a, c, count(b) as cb
	RETURN a.nid, c.nid, ((cb*1.0)/(a.neighbors+c.neighbors-cb)) as score
	ORDER BY score DESC
	LIMIT 100
"""

# xAdamicAdar Cypher doens't support log function or UDF

bPreferentialAttachment = '''
	START a=node:nodes(istopn="true"), c=node:nodes(istopn="true")
	WHERE a.nid < c.nid
	RETURN a.nid, c.nid, a.neighbors*c.neighbors as score 
	ORDER BY score DESC
	LIMIT 100
'''

xPreferentialAttachment = '''
	START a=node({n}), c=node:nodes(istopn="true")
	WHERE a <> c
	RETURN a.nid, c.nid, a.neighbors*c.neighbors as score 
	ORDER BY score DESC
	LIMIT 100
'''

# xKatz Cypher doens't support power function or UDF

xGraphDistance = """
	START a=node({n}) 
	MATCH (a)-[:COAUTH*..%d]->(c) 
	WHERE a <> c 
	WITH DISTINCT a, c 
	MATCH p=ShortestPath((a)-[:COAUTH*..%d]->(c)) 
	RETURN a.nid, c.nid, length(p) AS score 
	ORDER BY score,c.nid ASC 
	LIMIT %d
"""

pGetNID = "start a=node({n}) return a.nid"
xRootedPageRankInit = "start n=node(*) where has(n.nid) set n.rpr = %0.16f"
xRootedPageRankSetSource = "start x=node({n}) match (x)-[:COAUTH]->(y) with (1-%f)+%f*sum(y.rpr/y.neighbors) as nrpr,x set x.nrpr=nrpr"
xRootedPageRankSetOther = "start x=node(*) where has(x.nid) with x match (x)-[:COAUTH]->(y) where x.nid < y.nid and x.nid <> %d with (1-%f)+%f*sum(y.rpr/y.neighbors) as nrpr,x set x.nrpr=nrpr"
xRootedPageRankSwitch = "start n=node(*) where has (n.nrpr) set n.rpr = n.nrpr"
xRootedPageRankQueryTop = "start y=node(*) where has(y.rpr) and y.nid <> %d return y.nid, y.rpr order by y.rpr desc limit 100"

def getNodeCount(graph_db):
	return cypher.execute(graph_db, pGetNodeCount)[0][0][0]

@benchmark
def generateTopNIndex(graph_db):
	nodes = graph_db.get_or_create_index(neo4j.Node, "nodes")
	for n in [n[0] for n in cypher.execute(graph_db, pGetTopN)[0]]:
		nodes.add("istopn", "true", n)

@benchmark
def benchmarkCypher(graph_db, query, params):
	cypher.execute(graph_db, query, params, error_handler=print)

def randomLoopBenchmark(graph_db, query, node_count, loop_count):
	if loop_count == 0: 
		return
	l = []
	t = time.time()
	for i in xrange(loop_count):
		if callable(query):
			l.append(query(graph_db,{'n': randint(1,node_count-1)}))
		else:
			l.append(benchmarkCypher(graph_db, query, {'n': randint(1,node_count-1)}))
	t = time.time()-t
	l.sort()
	return l[0], l[len(l)/2], l[-1], t	# min, median, max, total

@benchmark
def tGraphDistance(graph_db, params):
	lim = 100
	pret = None
	for d in xrange(1,5):
		ret, meta = cypher.execute(graph_db, xGraphDistance % (d,d,lim), params, error_handler=print)
		if len(ret) != lim and ret != pret:
			pret = ret
		else:
			return

@benchmark
def tCommonNeighbors(graph_db):
	ret, meta = cypher.execute(graph_db, "start a=node(*) return max(a.neighbors!)")
	threshold = ret[0][0] or 0
	while (threshold>0):
		threshold /= 2
		ret, meta = cypher.execute(graph_db, bCommonNeighbors % threshold, {}, error_handler=print)
		if ret[-1][2] > threshold:
			return

@benchmark
def tRootedPageRank(graph_db, params):
	d = 0.85	# PageRank Damping factor
	pret, ret, cret = [], [], None
	N = getNodeCount(graph_db)
	xnid = cypher.execute(graph_db, pGetNID, params)[0][0][0]
	cypher.execute(graph_db, xRootedPageRankInit % (1.0/N))
	while (pret != cret):
		pret = cret
		cypher.execute(graph_db, xRootedPageRankSetSource % (d,d), params)
		cypher.execute(graph_db, xRootedPageRankSetOther %(xnid,d,d))
		cypher.execute(graph_db, xRootedPageRankSwitch)
		ret, meta = cypher.execute(graph_db, xRootedPageRankQueryTop % xnid)
		cret = [nid for nid,score in ret]

def main():
	graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
	node_count = getNodeCount(graph_db)
	
	print("addNeighborsProperty", benchmarkCypher(graph_db, pInitNeighbors, {}) + benchmarkCypher(graph_db, pSetNeighbors, {}))
	print("generateTopNIndex", generateTopNIndex(graph_db))
	print("bCommonNeighbors", tCommonNeighbors(graph_db))
	print("xCommonNeighbors", randomLoopBenchmark(graph_db, xCommonNeighbors, node_count, 1000))
	print("xJaccardsCoefficient", randomLoopBenchmark(graph_db, xJaccardsCoefficient, node_count, 1000))
	print("xGraphDistance", randomLoopBenchmark(graph_db, tGraphDistance, node_count, 1000))
	print("xPreferentialAttachment", randomLoopBenchmark(graph_db, xPreferentialAttachment, node_count, 1000))
	print("bPreferentialAttachment", benchmarkCypher(graph_db, bPreferentialAttachment, {}))
	print("xRootedPageRank", randomLoopBenchmark(graph_db, tRootedPageRank, node_count, 10))

if __name__ == "__main__":
	main()
