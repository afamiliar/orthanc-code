# This script is for transferring data from a source Orthanc to a destination Orthanc instance
#       (1) define a list of uuids for studies to transfer
#           making sure don't process same study twice
#       (2) loop through each study:
#           Download to local as a .zip (from source)
#           Upload (to destination)
#           Remove local .zip
#       (3) Update log of completed study uuids
#
#   amf, Nov 2021

import json
import os
import pandas as pd
from custom import orthanc

hpc_url = orthanc.get_orthanc_url('HPC') # source instance
aws_url = orthanc.get_orthanc_url('AWS') # destination instance

# process *all* studies on the source instance
study_uuids = orthanc.all_study_uuids(hpc_url) 

# process studies based on a list of accession #s
study_uuids=[]
accession_df = pd.read_csv('accessions_cranio.csv')
for ind,row in accession_df.iterrows():
    uuids = orthanc.get_uuids_from_accession(hpc_url,row['accession_num'])
    study_uuids = study_uuids + uuids

# load record of transferred studies
log='uuids_transferred.json'
if os.path.exists(log):
    done=json.load(open(log))
else:
    done=[]

# start the process
study_uuids = list(set(study_uuids) - set(done)) # remove any completed uuids from uuids_to_process
for uuid in study_uuids:
    out_name=uuid+'.zip'
    orthanc.download_study(hpc_url,uuid,out_name)
    orthanc.UploadZip(out_name,aws_url)
    done.append(uuid)
    os.remove(out_name)

# update the record of completed studies
with open(log, 'w') as f:
    json.dump(done, f)

