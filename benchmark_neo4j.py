#!/usr/bin/env python2.7

from __future__ import print_function
from random import randint
from py2neo import neo4j, rest, cypher
from GDBB_Commons import *

import json
import time
import sys

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
	WHERE has(a.nid)
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

pGraphNIDtoNeoNodeID = 'start n=node(*) where has(n.nid) and n.nid = %s return n'

pGetNID = "start a=node({n}) return a.nid"
xRootedPageRankInit = "start n=node(*) where has(n.nid) set n.rpr = %0.16f"
xRootedPageRankSetSource = "start x=node({n}) match (x)-[:COAUTH]->(y) with (1-%f)+%f*sum(y.rpr/y.neighbors) as nrpr,x set x.nrpr=nrpr"
xRootedPageRankSetOther = "start x=node(*) where has(x.nid) with x match (x)-[:COAUTH]->(y) where x.nid < y.nid and x.nid <> %d with %f*sum(y.rpr/y.neighbors) as nrpr,x set x.nrpr=nrpr"
xRootedPageRankSwitch = "start n=node(*) where has (n.nrpr) set n.rpr = n.nrpr"
xRootedPageRankQueryTop = "start y=node(*) where has(y.rpr) and y.nid <> %d return y.nid, y.rpr order by y.rpr desc limit %d"

xKatzInit = "start n=node(*) where has(n.nid) set n.katz_s = 0"
xKatzBaseItr = "start n=node({n}) match p=(n)-[:COAUTH*%d..%d]->(y) where y <> n  with y, count(p)%s as s set y.katz_s = y.katz_s + s"
xKatzQueryTop = "start n=node(*) where has(n.katz_s) and n.katz_s > 0 return n.nid, n.katz_s order by n.katz_s desc limit %d"

def xKatzGenerateCypher(beta, depth):
	betaMul = ("*"+str(beta))*depth
	return xKatzBaseItr%(depth,depth,betaMul)

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

__getNeo4JRandomNodes = None
def getNeo4JRandomNodes(graph_db, dataset, count):
	''' Using GDBB_Commons.getRandomNodes and translating node ID to Neo4J's id
	'''
	global __getNeo4JRandomNodes
	if __getNeo4JRandomNodes is not None and len(__getNeo4JRandomNodes)>=count:
		return __getNeo4JRandomNodes[0:count]
	__getNeo4JRandomNodes = []
	nid_random_nodes = getRandomNodes(dataset, count=count)
	for n in nid_random_nodes:
		ret, meta = cypher.execute(graph_db, pGraphNIDtoNeoNodeID%n)
		if type(ret) is list and len(ret) == 1:
			if type(ret[0]) is list and len(ret[0]) == 1:
				__getNeo4JRandomNodes.append(ret[0][0].id)
	if len(nid_random_nodes) != len(__getNeo4JRandomNodes):
		sys.stderr.write('unable to match all random nodes in Neo4J')
	return __getNeo4JRandomNodes[:]


def randomLoopBenchmark(graph_db, query, dataset, loop_count):
	if loop_count == 0: 
		return
	random_nodes = getNeo4JRandomNodes(graph_db, dataset, loop_count)
	l = []
	t = time.time()
	for n in random_nodes:
		if callable(query):
			l.append(query(graph_db,{'n': n}))
		else:
			l.append(benchmarkCypher(graph_db, query, {'n': n}))
	t = time.time()-t
	l.sort()
	return {'min':l[0], 'median':l[len(l)/2], 'max':l[-1], 'total':t, 'count':loop_count}	# min, median, max, total

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
def tKatz(graph_db, params):
	beta = 0.1
	maxDepth = 3
	cypher.execute(graph_db, xKatzInit)
	for i in xrange(1,maxDepth+1):
		cypher.execute(graph_db, xKatzGenerateCypher(0.1,i), params)
	#cypher.execute(graph_db, xKatzQueryTop%100)
	

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
		cypher.execute(graph_db, xRootedPageRankSetOther %(xnid,d))
		cypher.execute(graph_db, xRootedPageRankSwitch)
		ret, meta = cypher.execute(graph_db, xRootedPageRankQueryTop % (xnid,100))
		cret = [nid for nid,score in ret]

def main():
	if len(sys.argv) != 2:
		print("you must specify dataset")
	dataset = sys.argv[1]
	graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
	node_count = getNodeCount(graph_db)
	ret = {}
	ret["cNeighborsIndex"] = (benchmarkCypher(graph_db, pInitNeighbors, {}) + benchmarkCypher(graph_db, pSetNeighbors, {}))
	ret["cTopNIndex"] = generateTopNIndex(graph_db)
	ret["bCommonNeighbors"] = tCommonNeighbors(graph_db)
	ret["bPreferentialAttachment"] = benchmarkCypher(graph_db, bPreferentialAttachment, {})
	ret["xCommonNeighbors"] =  randomLoopBenchmark(graph_db, xCommonNeighbors, dataset, 1000)
	ret["xJaccardsCoefficient"] =  randomLoopBenchmark(graph_db, xJaccardsCoefficient, dataset, 1000)
	ret["xGraphDistance"] =  randomLoopBenchmark(graph_db, tGraphDistance, dataset, 1000)
	ret["xPreferentialAttachment"] =  randomLoopBenchmark(graph_db, xPreferentialAttachment, dataset, 1000)
	ret["xKatz"] =  randomLoopBenchmark(graph_db, tKatz, dataset, 100)
	ret["xRootedPageRank"] =  randomLoopBenchmark(graph_db, tRootedPageRank, dataset, 10)
	print(json.dumps(ret))

if __name__ == "__main__":
	main()
