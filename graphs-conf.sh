#!/bin/bash

# DBLP Graphs
declare -a DBLP_GRAPH_NAMES=( all_core3 2002_2009_core3 2010_2012_core3 )
declare -a DBLP_GRAPH_OPTS=( "--core 3" "-e 2009-12-31 --core 3" "-s 2010-01-01 --core 3" )

# SNAP Graphs
declare SNAP_BASE_URL="http://snap.stanford.edu/data"
declare -a SNAP_GRAPH_NAMES=( email-Enron ca-AstroPh ca-CondMat ca-GrQc ca-HepPh ca-HepTh facebook )
declare -a SNAP_GRAPH_OPTS=( "txt.gz" "txt.gz" "txt.gz" "txt.gz" "txt.gz" "txt.gz" "tar.gz" )

# All Graphs
declare -a ALL_GRAPH_NAMES=( "${DBLP_GRAPH_NAMES[@]}" "${SNAP_GRAPH_NAMES[@]}" )
