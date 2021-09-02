#!/bin/bash
#
# https://unix.stackexchange.com/questions/477210/looping-through-json-array-in-shell-script/477218
#
#   Downloads "studies" from Orthanc based on list of accession numbers
#   Accounts for multiple "studies" associated with the same accession number (appends _# to file name)
#   Ouputs zipped studies to data/ directory within current dir
#
#   Expects naming manifest within current directory
#       Defaults to file name "naming_manifest.csv"
#       Manifest must follow standard format
#
#   amf 6-30-2021
#
#   example usage:
#       ./download_data_new.sh naming_manifest.csv
#       ./download_data_new.sh

localhost=

## grab list of accession #s from the naming manifest to query Orthanc with
# download_list=(4472066) # accession #'s to include
download_list=()
file_name="naming_manifest.csv"
INPUT=${1:-$file_name} # if no input provided, use default file name
# INPUT="naming_manifest.csv"

## =============== if no CSV found, throw error =============== 
[ ! -f $INPUT ] && { echo "$INPUT file not found"; exit 99; }

## =============== loop through rows of CSV, grab accession number for the row & query Orthanc =============== 
echo "STATUS: Querying orthanc with input accession numbers"
OLDIFS=$IFS
IFS=','
while read flywheel_project subject_id  sdg_id imaging_type age_at_imaging   anatomical_position  access_number Radiology_Request || [ -n "$access_number" ]; do
    echo ${access_number}
    results=[]
    results=$(curl -s http://${localhost}:8042/tools/find -d '{"Level":"Study","Query":{"AccessionNumber":"'"$access_number"'"}}')
    if [[ ${results} == [] ]] ; then # if empty
        echo "MISSING: ${access_number}"
    else
        ## loop through UUIDs & download data
        for uuid in $(echo ${results} | jq -r '.[]'); do
            if [[ -f data/${access_number}.zip ]] ; then
                output_fn=${access_number}_${count}.zip
                curl -s http://${localhost}:8042/studies/${uuid}/archive > ${output_fn}
                let count=${count}+1
            else
                output_fn=${access_number}.zip
                curl -s http://${localhost}:8042/studies/${uuid}/archive > ${output_fn}                
                count=0
            fi

            if [[ ! -d data/ ]] ; then mkdir data ; fi
            mv ${output_fn} data/
        done
    fi
    
    let count=${count}+1
done < $INPUT
IFS=$OLDIFS
