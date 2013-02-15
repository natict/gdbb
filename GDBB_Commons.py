#!/usr/bin/env python2.7

from __future__ import print_function

import os
import sys
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

__getRandomNodesCache=None
def getRandomNodes(dataset, filename='rand_nodes.csv', count=1000):
	''' Return a list of random node ids from a given file 
		using a cache to allow efficiency over multiple calls 
	'''
	global __getRandomNodesCache
	if __getRandomNodesCache is not None and len(__getRandomNodesCache)>=count:
		return __getRandomNodesCache[0:count]
	ret = []
	lineCounter = 0
	try:
		with open(os.path.join(dataset, filename)) as f:
			for line in f:
				pair = line.split(',')
				if len(pair) == 2:
					ret.append(pair[0])
				if len(ret) >= count:
					break
	except:
		sys.stderr.write('unable to read random nodes file (%s)' % os.path.join(dataset, filename))
	__getRandomNodesCache=ret[:] # clone to cache
	return ret
