#!/usr/bin/python2.7

import argparse
import itertools
import math
import redis
import sys
import time

DEBUG = False
MAX_BUFFER_SIZE = 1000000

def dprint(msg):
	if DEBUG:
		print("DEBUG: " + str(msg))

class benchmark(object):
	def __init__(self, func):
		self.func = func
	def __call__(self, *args):
		dprint("running %s%s" %(self.func.__name__, str(args)))
		t = time.time()
		self.func(*args)
		dprint("time elapsed: %g" %(time.time()-t))

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
	def inc(self, k):
		self[k] = self.get(k,0) + 1

class TopNDict(dict):
	'''
		dict that only hold N highest values
		All keys are converted to int
		NOTE: not checking for value overwriting
	'''
	def __init__(self, N):
		self.N = max(N,1)
		self.min_key = None
		super(dict, self)

	def add(self, k, v):
		if len(self) >= self.N:
			if (self.min_key == None) or (v < self[self.min_key]):
				return
			else:
				self.pop(self.min_key)
		self[k] = v
		self.min_key = min(self.iterkeys(), key=lambda x: self[x])


def pjoin(p):
	sp = sorted([int(p[0]), int(p[1])])
	return str(sp[0])+','+str(sp[1])

@benchmark
def loadEdgesCSVToRedis(ajlist_db, csv_filename):
	ajlist_db.flushdb()
	bdb = BufferedRedisDBSets(ajlist_db, MAX_BUFFER_SIZE)
	with open(csv_filename) as f:
		for line in f:
			pair = line.strip().split(',')
			bdb.sadd(pair[0], pair[1])
	bdb.flush()

def printDictByVal(d, lim=None):
  	'''
    	print dict sorted by value with limit
  	'''
	if lim == None:	lim = len(d)

	sd = sorted(d.keys(), cmp=lambda x,y: cmp(d.get(x), d.get(y)), reverse=True)
	for i in xrange(min(lim, len(sd))):
		print sd[i], d.get(sd[i])

def get_Common_Neighbors_Counters(r, x):
	d = CounterDict()
	for n in r.smembers(x):
		for y in r.smembers(n):
			if str(x) != y:
				d.inc(y)
	return d

@benchmark
def b_Common_Neighbors(redis_interface, limit, output=False):
	lua_script = """
		local limit = tonumber(ARGV[1])
		local tc = {};
		for k, x in pairs(redis.call('keys', '*')) do
		  for k1,n in pairs(redis.call('smembers', x)) do 
			for k2,y in pairs(redis.call('smembers', n)) do
			  if x < y then
				tc[x..','..y] = (tc[x..','..y] or 0) + 1;
			  end;
			end;
		  end;
		end; 
		local ttop = {};
		local min = math.huge;
		local mini = '';
		for k,v in pairs(tc) do
		  if (#ttop < limit) then
			table.insert(ttop, {k,v});
			if v<min then min=v; mini=table.maxn(ttop); end;
		  else
			if v>min then
			  ttop[mini] = {k,v};
			  min = math.huge;
			  for i = 1,#ttop,1 do
				if ttop[i][2]<min then min=ttop[i][2]; mini=i;  end;
			  end;
			end;
		  end;
		end;
		table.sort(ttop, function (a,b) return a[2]>b[2]; end);
		local tret = {};
		for i = 1,#ttop,1 do
		  table.insert(tret, ttop[i][1]..','..ttop[i][2]);
		end;
		return tret;
	"""
	getCN = redis_interface.register_script(lua_script)
	for triplet in getCN(keys=[], args=[limit]):
		tf = triplet.split(',')
		if output and len(tf)==3: 
			print('%s,%s\t%s' % tuple(tf))

@benchmark
def b_Jaccards_Coefficient(redis_interface, limit, output=False):
	lua_script = """
		local limit = tonumber(ARGV[1])
		local tc = {};
		for k, x in pairs(redis.call('keys', '*')) do
		  for k1,n in pairs(redis.call('smembers', x)) do 
				for k2,y in pairs(redis.call('smembers', n)) do
				  if x < y then
						tc[x..','..y] = (tc[x..','..y] or 0) + 1;
				  end;
				end;
		  end;
		end; 
		local function jc(k,v)
		  local sp = k:find(',');
		  local xn = redis.call('scard',(k:sub(0,sp-1)));
		  local yn = redis.call('scard',(k:sub(sp+1,#k)));
		  return (v/(xn+yn-v));
		end;
		local ttop = {};
		local min = math.huge;
		local mini = '';
		local jcv = 0;
		for k,v in pairs(tc) do
		  if (#ttop < limit) then
				jcv = jc(k,v);
				table.insert(ttop, {k,jcv});
				if jcv<min then min=jcv; mini=table.maxn(ttop); end;
		  else
				jcv = jc(k,v);
				if jcv>min then
				  ttop[mini] = {k,jcv};
				  min = math.huge;
				  for i = 1,#ttop,1 do
						if ttop[i][2]<min then min=ttop[i][2]; mini=i;  end;
				  end;
				end;
		  end;
		end;
		table.sort(ttop, function (a,b) return a[2]>b[2]; end);
		local tret = {};
		for i = 1,#ttop,1 do
		  table.insert(tret, ttop[i][1]..','..ttop[i][2]);
		end;
		return tret;
	"""
	getJC = redis_interface.register_script(lua_script)
	for triplet in getJC(keys=[], args=[limit]):
		tf = triplet.split(',')
		if output and len(tf)==3: 
			print('%s,%s\t%s' % tuple(tf))

@benchmark
def b_Adamic_Adar(redis_interface, limit, output=False):
	lua_script = """
		local limit = tonumber(ARGV[1]);
		local tc = {};
		for k, x in pairs(redis.call('keys', '*')) do
		  for k1,n in pairs(redis.call('smembers', x)) do 
			for k2,y in pairs(redis.call('smembers', n)) do
			  if x < y then
				if not tc[x..','..y] then
				  tc[x..','..y] = {};
				end;
				table.insert(tc[x..','..y], n);
			  end;
			end;
		  end;
		end;
		local function aa(k,v)
		  local sum = 0;
		  for i,z in pairs(v) do
			local zn = redis.call('scard',z);
			sum = sum + 1/math.log10(zn);
		  end;
		  return sum;
		end;
		local ttop = {};
		local min = math.huge;
		local mini = '';
		local aav = 0;
		for k,v in pairs(tc) do
		  aav = aa(k,v);
		  if (#ttop < limit) then      
			table.insert(ttop, {k,aav});
			if aav<min then min=aav; mini=table.maxn(ttop); end;
		  else
			if aav>min then
			  ttop[mini] = {k,aav};
			  min = math.huge;
			  for i = 1,#ttop,1 do
				if ttop[i][2]<min then min=ttop[i][2]; mini=i;  end;
			  end;
			end;
		  end;
		end;
		table.sort(ttop, function (a,b) return a[2]>b[2]; end);
		local tret = {};
		for i = 1,#ttop,1 do
		  table.insert(tret, ttop[i][1]..','..ttop[i][2]);
		end;
		return tret;
	"""
	getAA = redis_interface.register_script(lua_script)
	for triplet in getAA(keys=[], args=[limit]):
		tf = triplet.split(',')
		if output and len(tf)==3: 
			print('%s,%s\t%s' % tuple(tf))


@benchmark
def b_Preferential_Attachment (redis_interface, limit, output=False):
	'''
		Preferential Attachment
	'''
	#Generate cache dict
	t = int(math.ceil((1+math.sqrt(1+8*(limit+1)))/2)) # reverse binomial coefficient
	dt = TopNDict(t)
	for y in redis_interface.keys():
		dt.add(y, redis_interface.scard(y))
	d = {}
	for c in itertools.combinations(dt, 2):
		d[c] = dt[c[0]] * dt[c[1]]
	if output: printDictByVal(d, limit)

def x_Common_Neighbors(redis_interface, x, limit, output=False):
	'''
		Common Neighbors for specific node
	'''
	d = get_Common_Neighbors_Counters(redis_interface, x)
	if output: printDictByVal(d, limit)

def x_Jaccards_Coefficient(redis_interface, x, limit, output=False):
	'''
		Jaccard's Coefficient for specific node
	'''
	nx = len(redis_interface.smembers(x))
	d = get_Common_Neighbors_Counters(redis_interface, x)
	for y in d:
		d[y] = float(d.get(y))/(nx + redis_interface.scard(y) - d.get(y))
	if output: printDictByVal(d, limit)

def get_Common_Neighbors(r, x):
	d = CounterDict()
	for n in r.smembers(x):
		for y in r.smembers(n):
			if str(x) != y:
				if not d.get(y): d[y] = set([])
				d[y].add(n)
	return d

def x_Adamic_Adar(redis_interface, x, limit, output=False):
	'''
		Adamic/Adar for specific node
	'''
	d = get_Common_Neighbors(redis_interface, x)
	for y in d:
		d[y] = sum(map(lambda z: float(1)/math.log10(redis_interface.scard(z)), d[y]))
	if output: printDictByVal(d, limit)

def x_Preferential_Attachment (redis_interface, x, limit, cache_db, output=False):
	'''
		Preferential Attachment for specific node
	'''
	if not (0 < cache_db.dbsize() <= limit+1):
		#Generate cache DB
		d = TopNDict(limit+1)
		cache_db.flushdb()
		for y in redis_interface.keys():
			if y != x: d.add(y, redis_interface.scard(y))
		for y in d.iterkeys():
			cache_db.set(y, d[y])
	nx = len(redis_interface.smembers(x))
	d = {}
	for y in cache_db.keys():
		if int(y) != x: d[int(y)] = int(cache_db.get(y))*nx
	if output: printDictByVal(d, limit)

def x_Graph_Distance(redis_interface, x, limit, output=False):
	'''
		Graph Distance for specific node (unlimited depth!)
	'''
	depth = 0
	d = {x: depth}
	prev_len = -1
	while (len(d) != prev_len) and (len(d) < limit+1):
		prev_len = len(d)
		depth -= 1
		for z in [ k for k in d.keys() if d[k] == depth+1 ]:
			for y in redis_interface.smembers(z):
				if not d.has_key(int(y)):
					d[int(y)] = depth
	d.pop(x)
	if output: printDictByVal(d, limit)

def x_Katz_Lua(redis_interface, x, limit, max_depth, beta, output=False):
	lua_script = """
		local x = tostring(KEYS[1]);
		local limit = tonumber(ARGV[1]);
		local max_depth = tonumber(ARGV[2]);
		local beta = tonumber(ARGV[3]);
		local paths = {};
		local reachables = {};
		local l = 0;
		paths[l] = {};
		paths[l][x] = 1;
		while l < max_depth do
		  l = l + 1;
		  paths[l] = {};
		  for z,vz in pairs(paths[l-1]) do
			for k,y in pairs(redis.call('smembers', z)) do
			  paths[l][y] = (paths[l][y] or 0) + vz;
			  if y ~= x then reachables[y] = 0; end;
			end;
		  end;
		end;
		local function katz(y)
		  local kval = 0;
		  for l = 1,max_depth,1 do
			kval = kval + (beta^l)*(paths[l][y] or 0);
		  end;
		  return kval;
		end;
		local ttop = {};
		local min = math.huge;
		local mini = '';
		local tmpval = 0;
		for k,v in pairs(reachables) do
		  tmpval = katz(k);
		  if (#ttop < limit) then      
			table.insert(ttop, {k,tmpval});
			if tmpval<min then min=tmpval; mini=table.maxn(ttop); end;
		  else
			if tmpval>min then
			  ttop[mini] = {k,tmpval};
			  min = math.huge;
			  for i = 1,#ttop,1 do
				if ttop[i][2]<min then min=ttop[i][2]; mini=i;  end;
			  end;
			end;
		  end;
		end;
		table.sort(ttop, function (a,b) return a[2]>b[2]; end);
		local tret = {};
		for i = 1,#ttop,1 do
		  table.insert(tret, ttop[i][1]..','..ttop[i][2]);
		end;
		return tret;
	"""
	KATZ_SCRIPT = redis_interface.register_script(lua_script)
	for triplet in KATZ_SCRIPT(keys=[x], args=[limit, max_depth, beta]):
		tf = triplet.split(',')
		if output and len(tf)==3: 
			print('%s,%s\t%s' % tuple(tf))

def x_Katz(redis_interface, x, limit, max_depth, beta, output=False):
	'''
		Katz (unweighted) for specific node
	'''
	paths = {}
	l = 0
	paths[l] = {x: 1}
	d = {}
	while l < max_depth:
		l += 1
		paths[l] = {}
		for z in paths[l-1].iterkeys():
			for y in [ int(n) for n in redis_interface.smembers(z)]:
				paths[l][y] = paths[l].get(y, 0) +  paths[l-1][z]
				if y != x: d[y] = 0
	for y in d.iterkeys():
		for l in xrange(1,max_depth+1):
			d[y] += (beta**l)*(paths[l].get(y, 0))
	if output: printDictByVal(d, limit)

# NOTE: RootedPageRank in Redis are a bit different from the other DBs,
#	because we dropped the 0-neighbors nodes on import (can't have empty set as value),
#	making N a bit smaller...
def x_RootedPageRank(redis_interface, x, limit, output=False):
	rpr_lua = '''
		local d = 0.85;
		local limit = tonumber(ARGV[1])
		local rpr = {};
		local nrpr = {};
		local N = redis.call('dbsize');
		local x = tostring(KEYS[1]);
		local ttop = {};
		local pttop = {0};
		local function rprsigma(n, t)
			local sum = 0;
			for k2, y in pairs(redis.call('smembers', n)) do
				sum = sum + t[y]/redis.call('scard', y);
			end;
			return d*sum;
		end;
		local function tcomp(t1,t2) 
			if (#t1 ~= #t2) then return false; end; 
			for i = 1,#t1,1 do 
				if (t1[i] ~= t2[i]) then return false; end; 
			end; 
			return true; 
		end;
		for k, n in pairs(redis.call('keys', '*')) do
			nrpr[n] = 1/N;
		end;
		while (not tcomp(ttop, pttop)) do
			pttop = ttop; ttop = {};
			rpr = nrpr; nrpr = {};
			local min = math.huge;
			local mini = '';
			for k, n in pairs(redis.call('keys', '*')) do
				nrpr[n] = rprsigma(n, rpr);
				if n == x then 
					nrpr[n] = nrpr[n] + (1-d); 
				else
					if (#ttop < limit) then
						table.insert(ttop, n);
						if nrpr[n]<min then min=nrpr[n]; mini=table.maxn(ttop); end;
					else
						if nrpr[n]>min then
							ttop[mini] = n;
							min = math.huge;
							for i = 1,#ttop,1 do
								if nrpr[ttop[i]]<min then min=nrpr[ttop[i]]; mini=i; end;
							end;
						end;
					end;
				end;
			end;
		end;
		table.sort(ttop, function (a,b) return nrpr[a]>nrpr[b]; end);
		local tret = {};
		for i = 1,#ttop,1 do
		  table.insert(tret, ttop[i]..','..tostring(nrpr[ttop[i]]));
		end;
		return tret;
	'''
	RPR_SCRIPT = redis_interface.register_script(rpr_lua)
	for val in RPR_SCRIPT(keys=[x], args=[limit]):
		fields = val.split(',')
		if output and len(fields)==2: 
			print('%s\t%s' % tuple(fields))

def parseArgs():
	parser = argparse.ArgumentParser(
			description='Load graph data into Redis')
	parser.add_argument('-H', dest='hostname', default='localhost', 
			help='Redis server hostname')
	parser.add_argument('-p', dest='port', default=6379, type=int,
			help='Redis server port')
	parser.add_argument('-f', dest='filename', default=None, 
			help='Edges CSV file to load')
	parser.add_argument('--verbose', '-v', action='count')
	args = parser.parse_args()
	if args.verbose > 0:
		global DEBUG
		DEBUG = True
	if not args.filename:
		dprint("you did not supply an edges file...")
	return args

def timer(t = None):
	if t == None:
		return time.time()
	else:
		return (time.time() - t)

def benchmarkFunctionLoop(func, count, desc, randfunc, kwargs):
	dprint(desc)
	timing_arr = []
	total = timer()
	for i in xrange(count):
		t = timer()
		func(x=randfunc(), **kwargs)
		timing_arr.append(timer(t))
	timing_arr.sort()
	if count > 2:
		dprint("Min: %d, Max: %d, Median: %d, Total: %d" % (timing_arr[0], timing_arr[-1], timing_arr[len(timing_arr)/2], timer(total)))

def main():
	args = parseArgs()

	rajl = redis.StrictRedis(host=args.hostname, port=args.port, db=0)
	rcn = redis.StrictRedis(host=args.hostname, port=args.port, db=1)
	rcache = redis.StrictRedis(host=args.hostname, port=args.port, db=2)
	rcache.flushdb()

	if args.filename: 
		loadEdgesCSVToRedis(rajl, args.filename)

	b_Common_Neighbors(rajl, 100)

	b_Jaccards_Coefficient(rajl, 100)
	
	b_Adamic_Adar(rajl, 100)
	
	b_Preferential_Attachment(rajl, 100)
	
	benchmarkFunctionLoop(x_Common_Neighbors, 1000, "Common Neighbors for node", 
			rajl.randomkey, {'redis_interface': rajl, 'limit': 100})
	benchmarkFunctionLoop(x_Jaccards_Coefficient, 1000, "Jaccard's Coefficient for node", 
			rajl.randomkey, {'redis_interface': rajl, 'limit': 100})
	benchmarkFunctionLoop(x_Adamic_Adar, 1000, "Adamic/Adar for node", 
			rajl.randomkey, {'redis_interface': rajl, 'limit': 100})
	benchmarkFunctionLoop(x_Preferential_Attachment, 1000, "Preferential Attachment for node", 
			rajl.randomkey, {'redis_interface': rajl, 'limit': 100, 'cache_db': rcache})
	benchmarkFunctionLoop(x_Graph_Distance, 1000, "Graph Distance for node", 
			rajl.randomkey, {'redis_interface': rajl, 'limit': 100})
	benchmarkFunctionLoop(x_Katz_Lua, 1000, "Katz (unweighted) for node", 
			rajl.randomkey, {'redis_interface': rajl, 'limit': 100, 'beta': 0.1, 'max_depth': 4})
	benchmarkFunctionLoop(x_RootedPageRank, 10, "RootedPageRank for node", 
			rajl.randomkey, {'redis_interface': rajl, 'limit': 100})

if __name__ == "__main__":
	main()
