#!/bin/bash
#
#   amf 06-2021
#
#   Takes list of accession numbers from input CSV (accession, uuid)
#   and checks for those on Orthanc using query capability.
#   Outputs accession #s missing on Orthanc to command line.
#
#   -- assumes there is a header row in the input CSV
#   -- accounts for the possibility that there are multiple Orthanc 'studies'
#       with the same accession number as long as they are duplicated in the input
#           (compares number of Orthanc UUIDs with the accession against 
#           number of times that accession is listed in the input)
#
# example usage: sh check_for_missing_data_noDownload.sh input.csv
#

localhost=


## =============== initialize stuff =============== 
download_list=()
checked_list=()
found=0
count=0
INPUT=$1 # "input_what_d3bgets-batch3.csv"

## =============== if no CSV found, throw error =============== 
[ ! -f $INPUT ] && { echo "$INPUT file not found"; exit 99; }

## =============== loop through rows of CSV, grab accession number for the row & query Orthanc =============== 
echo "STATUS: Querying orthanc with input accession numbers"
OLDIFS=$IFS
IFS=','
while read access_number corr_uuid || [ -n "$access_number" ]; do
    results=[]
    results=$(curl -s http://${localhost}:8042/tools/find -d '{"Level":"Study","Query":{"AccessionNumber":"'"$access_number"'"}}')
    if [[ ${results} == [] ]] ; then # if empty
        echo "MISSING: ${access_number}"
    else
        # echo "Found: ${access_number}"
        checked_list+=($access_number)
        let found=${found}+1
    fi
    
    let count=${count}+1
done < $INPUT
IFS=$OLDIFS


## =============== account for duplicate accession numbers with multiple studies =============== 
## this is a bit messy...
# echo "STATUS: checking for duplicate accession numbers"

# get list of accesion #s and counts
counts=$(echo ${checked_list[@]} | tr " " "\n" | uniq -c) 

# split list into separate variables
count_nums=()
accession_nums=()
for i in ${counts[@]} ; do
    if [ ${#i} == 1 ] ; then # if this is acount
        count_nums+=(${i})
    else # if it's an accession #
        accession_nums+=(${i})
    fi
done

# find accession #s with more than one corresponding UUID
ind=0
for i in ${count_nums[@]} ; do
    if [ ${i:0:1} != 1 ] ; then # if count is greater than 1
        access_number=${accession_nums[$ind]}
        results=$(curl -s http://${localhost}:8042/tools/find -d '{"Level":"Study","Query":{"AccessionNumber":"'"$access_number"'"}}')
        num_on_orthanc=$(echo ${results} | jq length) # get the number of uuids with this accession #
        if [ $num_on_orthanc != $i ] ; then # compare with expected number based on input
            let num_missing=$i-$num_on_orthanc
            let found=${found}-$num_missing
            echo "MISSING: $num_missing study with duplicate accession number $access_number"
        fi
    fi
    let ind=${ind}+1
done


## =============== finish up =============== 
echo "STATUS: finishing up"

let count=${count}-1 # remove header

echo "Found: ${found}"
echo "Out of: ${count}"
