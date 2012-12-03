#!/usr/bin/python2.7

import argparse
import itertools
import redis
import time

DEBUG = False
MAX_BUFFER_SIZE = 1000000

class BufferedRedisDBSets(dict):
	'''
		Wrapping dict to add-to-set and flush to Redis DB
	'''
	def __init__(self, db, maxbuf):
		self.db = db
		self.maxbuf = maxbuf
		self.count = 0
		super(dict, self)
	
	def sadd(self, key, value):
		if not self.has_key(key): 
			self[key] = set([])
		self[key].add(value)

		if self.count < self.maxbuf:
			self.count += 1
		else:
			self.flush()
	
	def flush(self):
		if self.count == 0:
			return
		with self.db.pipeline() as pipe:
			pipe.multi()
			for k in self.keys():
				pipe.sadd(k, *(self.get(k)))
			pipe.execute()
		try: 
			self.db.bgsave()
		except:
			pass
		self.clear()
		self.count = 0

class CounterDict(dict):
	'''
		dict of counters
	'''
	def inc(k):
		self[k] = self.get(k,0) + 1

def pjoin(p):
	sp = sorted([int(p[0]), int(p[1])])
	return str(sp[0])+','+str(sp[1])

def loadEdgesCSVToRedis(ajlist_db, csv_filename):
	ajlist_db.flushdb()
	bdb = BufferedRedisDBSets(ajlist_db, MAX_BUFFER_SIZE)
	with open(csv_filename) as f:
		for line in f:
			pair = line.strip().split(',')
			bdb.sadd(pair[0], pair[1])
	bdb.flush()

def createCommonNeighbors(ajlist_db, cn_db):
	cn_db.flushdb()
	ajl_keys = ajlist_db.keys()
	bdb = BufferedRedisDBSets(cn_db, 10000)
	for k in ajl_keys:
		ci = itertools.combinations(ajlist_db.smembers(k),2)
		for c in ci:
			bdb.sadd(pjoin(c), k)
	bdb.flush()

def printDictByVal(d, lim=None):
  	'''
    	print dict sorted by value with limit
  	'''
	if lim == None:	lim = len(d)

	sd = sorted(d, cmp=lambda x,y: cmp(d.get(x), d.get(y)), reverse=True)
	for i in xrange(min(lim, len(sd))):
		print sd[i], d.get(sd[i])

def get_Common_Neighbors_Counters(r, x):
	d = CounterDict()
	for n in r.smembers(x):
		for y in r.smembers(n):
			if str(x) != y:
				d.inc(y)
    
def x_Common_Neighbors(r, x, lim):
	'''
		Common Neighbors for specific node
	'''
	d = get_Common_Neighbors_Counters(r, x)
	printDictByVal(d, lim)

def x_Jaccards_Coefficient(r, x, lim):
	'''
		Jaccard's Coefficient for specific node
	'''
	nx = r.smembers(x)
	d = get_Common_Neighbors_Counters(r, x)
	for y in d:
		d[y] = float(d.get(y))/(len(nx) + r.scard(y) - d.get(y))
	printDictByVal(d, lim)

def parseArgs():
	parser = argparse.ArgumentParser(
			description='Load graph data into Redis')
	parser.add_argument('-H', dest='hostname', default='localhost', 
			help='Redis server hostname')
	parser.add_argument('-p', dest='port', default=6379, type=int,
			help='Redis server port')
	parser.add_argument('-f', dest='filename', required=True, 
			help='Edges CSV file to load')
	parser.add_argument('--verbose', '-v', action='count')
	args = parser.parse_args()
	if args.verbose > 0:
		global DEBUG
		DEBUG = True
	return args

def timer(t = None):
	if t == None:
		return time.time()
	else:
		return (time.time() - t)

def dprint(msg):
	if DEBUG:
		print("DEBUG: " + str(msg))

def benchmarkFunction(func, args, desc):
	dprint(desc)
	t = timer()
	func(*args)
	dprint("time elapsed: " + str(timer(t)))

def main():
	args = parseArgs()

	rajl = redis.StrictRedis(host=args.hostname, port=args.port, db=0)
	rcn = redis.StrictRedis(host=args.hostname, port=args.port, db=1)

	benchmarkFunction(loadEdgesCSVToRedis, (rajl, args.filename), "Loading edges")
	benchmarkFunction(createCommonNeighbors, (rajl, rcn), "Creating Common Neighbors DB")


if __name__ == "__main__":
	main()
