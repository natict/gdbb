#!/usr/bin/env python2.7

from __future__ import print_function

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
