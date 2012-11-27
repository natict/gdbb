#! /bin/bash

function tdiff() {
	local stime=$1
	local etime=$(date '+%s')

	if [[ -z "$stime" ]]; then stime=$etime; fi

	local dt=$((etime - stime))

	echo $dt
}

function tprint() {
	local dt=$1
	ds=$((dt % 60))
	dm=$(((dt / 60) % 60))
	dh=$((dt / 3600))
	printf '%d:%02d:%02d' $dh $dm $ds
}

function timer()
{
    if [[ $# -eq 0 ]]; then
        echo $(date '+%s')
    else
        tprint $(tdiff $1)
    fi
}
