#!/usr/bin/env python2.7

from __future__ import print_function

import json
import os
import sys

class DictOfLists(dict):
    def __init__(self):
        self.__names = []
        super(DictOfLists, self).__init__()

    def add(self, d, name):
        self.__names.append(name)
        for k,v in d.iteritems():
            if not self.has_key(k):
                self[k] = []
            self[k].append(v)

    def printCSV(self):
        print(','.join([''] + self.__names))
        for k in self.iterkeys():
            print(','.join([k] + [ str(e) for e in self[k]]))

def flatDict(d, sep, prefix=None):
    ret = {}
    for k,v in d.iteritems():
        sk = str(k)
        if prefix: 
            sk = prefix + str(sep) + sk
        if type(v) is dict:
            ret.update(flatDict(v, sep, sk))
        else:
            ret[sk] = v
    return ret


dol = DictOfLists()

for json_file in filter(lambda s: s.rfind('json')>=0, sys.argv):
    dol.add(flatDict(json.loads(open(json_file).read()),'.'), 
            os.path.basename(os.path.split(json_file)[0]))

dol.printCSV()
