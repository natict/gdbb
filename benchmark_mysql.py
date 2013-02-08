#!/usr/bin/env python2.7

from __future__ import print_function
from GDBB_Commons import benchmark

import json
import time
import MySQLdb

GET_RANDOM_NODES_SQL = """
	SELECT id 
	FROM nodes 
	ORDER BY RAND() 
	LIMIT %d
"""

@benchmark
def runMySQLProcedure(db, func_name, params=[], output=False):
	cur = db.cursor()
	cur.execute("CALL %s(%s)" % (func_name, ','.join([str(e) for e in params])))
	if output: print(cur.fetchall())
	cur.close()

def getRandomNodes(db, count):
	cur = db.cursor()
	cur.execute(GET_RANDOM_NODES_SQL % count)
	random_nodes = [e for t in cur.fetchall() for e in t]
	cur.close()
	return random_nodes

def randomLoopBenchmark(db, func_name, loop_count, params=[]):
	if loop_count == 0: return
	random_nodes = getRandomNodes(db, loop_count)
	l = []
	t = time.time()
	for nid in random_nodes:
		l.append(runMySQLProcedure(db, func_name, [nid]+params))
	t = time.time()-t
	l.sort()
	return {'min':l[0], 'median':l[len(l)/2], 'max':l[-1], 'total':t, 'count':loop_count}	# min, median, max, total

def main():
	db = MySQLdb.connect(user='gdbb', db='gdbb')
	ret = {}

	ret["cCommonNeighbors"] = runMySQLProcedure(db, "create_cn_table")
	ret["cCommonNeighborsCount"] = runMySQLProcedure(db, "create_cnc_table")
	ret["cNeighborsIndex"] = runMySQLProcedure(db, "create_neighbors_table")

	ret["bCommonNeighbors"] = runMySQLProcedure(db, "b_Common_Neighbors")
	ret["bJaccardsCoefficient"] = runMySQLProcedure(db, "b_Jaccard_Coefficient")
	ret["bAdamicAdar"] = runMySQLProcedure(db, "b_Adamic_Adar")
	ret["bPreferentialAttachment"] = runMySQLProcedure(db, "b_Preferential_attachment")

	ret["xCommonNeighbors"] = randomLoopBenchmark(db, "x_Common_Neighbors", 1000)
	ret["xJaccardsCoefficient"] = randomLoopBenchmark(db, "x_Jaccard_Coefficient", 1000)
	ret["xAdamicAdar"] = randomLoopBenchmark(db, "x_Adamic_Adar", 1000)
	ret["xPreferentialAttachment"] = randomLoopBenchmark(db, "x_Preferential_attachment", 1000)
	ret["xGraphDistance"] = randomLoopBenchmark(db, "x_Graph_Distance", 1000, [4,100])
	ret["xKatz"] = randomLoopBenchmark(db, "x_Katz", 100, [3,0.1,100])
	ret["xRootedPageRank"] = randomLoopBenchmark(db, "x_RootedPageRank", 10)

	print(json.dumps(ret))

if __name__ == "__main__":
	main()
