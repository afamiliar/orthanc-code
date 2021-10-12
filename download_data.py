## Downloads (all) DICOM files from Orthanc to s3
#   amf
#   Sept 2021
#
#       Gets a list of all studies on source Orthanc
#       Downloads each study to zip archive in target s3 bucket (named by the unique Orthanc uuid)

cred = '<user-password>' # orthanc credentials
ip = '<ip-address>'
port = '<dicom-port>'

localhost=cred+'@'+ip

import os
from pathlib import Path
import json

os.system('curl -s https://'+localhost+':'+port+'/studies > all_study_uuids_'+ip+'.json')

output_path='s3://d3b-phi-data-prd/imaging/radiology/DICOMs/'

count=1
with open('all_study_uuids_'+ip+'.json') as data_file:
    study_uuids = json.load(data_file)
    num_studies = len(study_uuids)
    for uuid in study_uuids:
        print('Downloading study '+str(count)+' out of '+str(num_studies))
        output_fn = uuid+'.zip'
        os.system('curl -s -k https://'+localhost+':'+port+'/studies/'+uuid+'/archive > '+output_fn)
        os.system('aws s3 cp '+output_fn+' '+output_path)
        print('Copied study to s3')
        count+=1
