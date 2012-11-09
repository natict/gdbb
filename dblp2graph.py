#! /usr/bin/python2.7
#
# This script extracts a graph from the DBLP XML
# 	Nodes - authors
#	Edges - between any two co-authors
#
# dblp2graph.py 
#		[-i <input-dir>]
#		[-o <output-dir>]
#		[-s <start-date>] 
#		[-e <end-date>] 
#		[--core <k>]
#
#	-i <input-dir>	Directory for dblp xml files (default: ./xml)
#	-o <output-dir>	Directory for nodes and edges files (default: ./)
# 	-s <start-date>	Only output edges & nodes from start-date (default: None)
#	-e <end-date>	Only output edges & nodes until end-date (default: None)
#	--core <k>		Only output nodes with <k> papers (default: 1)
#
#	example:
#		dblp2graph.py -o ./graph -s 2000-01-01 -e 2005-01-01 --core 3

# NOTE: This is a memory intensive script, approximately O(n)
#		Make sure you can fit the XML in memory

import argparse
import datetime
import itertools
# lxml can load elements from DTD file
from lxml import etree
import os

INPUT_XML = "dblp.xml"
INPUT_DTD = "dblp.dtd"
OUTPUT_NODES = "nodes.csv"
OUTPUT_EDGES = "edges.csv"
ISO_DATE_FORMAT = '%Y-%m-%d'
PAPER_TYPES = set(["article","inproceedings","proceedings","book",
		"incollection","phdthesis","mastersthesis","www"])
PAPER_DATE_ATT = 'mdate'
PAPER_AUTHOR_ELEM = 'author'

# dast: 
#	dict of author-name:publication-count
nodes = {}
#	set of co-author sets (using hash of author-name)
edges = set()
# This will enable O(n) memory footprint, 
# 	and allow to filter out the core.

# Checks if this is a valid paper
def isValidPaper(e, startDate=None, endDate=None):
	if not e.tag in PAPER_TYPES:
		return False
	# check for mdate attribute
	datestr = elem.get(PAPER_DATE_ATT)
	if not datestr:
		elem.clear()
		return False
	mdate = datetime.datetime.strptime(datestr, ISO_DATE_FORMAT)
	# validate startDate/endDate
	if (startDate and startDate > mdate) or (endDate and endDate < mdate):
		elem.clear()
		return False
	return True


# parse commandline arguments
parser = argparse.ArgumentParser(
		description='Extract a graph from the DBLP XML')
parser.add_argument('--core', dest='core', default=1, type=int, 
		help='Only output nodes with CORE papers')
parser.add_argument('-i', dest='inputDir', default='./xml', 
		help='Directory for dblp xml files')
parser.add_argument('-o', dest='outputDir', default='./', 
		help='Directory for nodes and edges files')
parser.add_argument('-s', dest='startDate', default=None, 
		help='Only output edges & nodes from STARTDATE')
parser.add_argument('-e', dest='endDate', default=None, 
		help='Only output edges & nodes until ENDDATE')
args = parser.parse_args()
if args.startDate:
	args.startDate = datetime.datetime.strptime(args.startDate, ISO_DATE_FORMAT)
if args.endDate:
	args.endDate = datetime.datetime.strptime(args.endDate, ISO_DATE_FORMAT)

# make sure we have dblp.xml, dblp.dtd exists
inputFiles = [ os.path.join(args.inputDir, INPUT_XML), 
		os.path.join(args.inputDir, INPUT_DTD) ]
for f in inputFiles:
	if not os.path.isfile(f):
		print("error: unable to find %s" % f)
		parser.print_help()
		exit(1)

context = etree.iterparse(
		os.path.join(args.inputDir, INPUT_XML), 
		load_dtd=True)

print('DEBUG: reading XML into data stracture')
uid = 0
for action, elem in context:
	if isValidPaper(elem, args.startDate, args.endDate):
		authors = [e for e in elem.getchildren() if e.tag == PAPER_AUTHOR_ELEM]
		if len(authors) > 1:
			# extract nodes
			authors_ids = set()
			#TODO: using python hash might expose the edges to corruption, use incremental index
			for author in authors:
				# remove commas and other non-supported characters for 
				#	a valid ASCII CSV output
				name = author.text.replace(',','').encode('utf-8').decode('ascii','ignore')
				if not nodes.has_key(name):
					nodes[name] = (uid, 0)
					uid += 1
				nodes[name] = (nodes[name][0], nodes[name][1]+1)
				authors_ids.add(nodes[name][0])
			# extract edges
			for e in itertools.combinations(authors_ids,2):
				edges.add(tuple(sorted(e)))
		# Free the RAM
		elem.clear()


# write data
if not os.path.exists(args.outputDir):
	os.makedirs(args.outputDir)

# write nodes
print('DEBUG: writing graph nodes')
nodesFile = open(os.path.join(args.outputDir, OUTPUT_NODES),'w')
nodesByID = {}
for k in nodes:
	if nodes[k][1] >= args.core:
		nodesFile.write('%s,%s\n' % (nodes[k][0],k))
		nodesByID[nodes[k][0]] = k
nodesFile.close()

# write edges
print('DEBUG: writing graph edges')
edgesFile = open(os.path.join(args.outputDir, OUTPUT_EDGES),'w')
for e in edges:
	if (e[0] in nodesByID) and (e[1] in nodesByID):
		edgesFile.write('%s,%s\n' % e)
edgesFile.close()
