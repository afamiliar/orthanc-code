
import requests
import json
import pandas as pd
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import os
import zipfile
import requests
import sys

def get_orthanc_url(flag):
# generate the appropriate url for a given instance (hard-coded)
    if flag == 'AWS':
        cred=
        ip=
        port=
        localhost=cred+'@'+ip+':'+port
        return 'https://'+localhost
    elif flag == 'HPC':
        cred=
        ip=
        port=
        localhost=cred+'@'+ip+':'+port
        return 'http://'+localhost

def download_study(orthanc_url,uuid,output_path):
# download a single study archive to local disk based on uuid
    os.system('curl -s -k '+orthanc_url+'/studies/'+uuid+'/archive > '+output_path)

def all_study_uuids(orthanc_url):
# get a list of all study uuids for a given instance
    return requests.get(orthanc_url+'/studies/', verify=False).json()

def get_uuids_from_accession(orthanc_url,accession):
# get the uuid(s) for a given accession number
    data = {'Level':'Study',
            'Query':{'AccessionNumber':str(accession)} }
    data_json = json.dumps(data)
    resp = requests.post(orthanc_url+'/tools/find', data=data_json, verify=False)
    return resp.json()

def get_study_metadata(orthanc_url,uuid):
# get the study metadata for a given uuid
    return requests.get(orthanc_url+'/studies/'+uuid+'/', verify=False).json()

def get_series_metadata(orthanc_url,uuid):
# get the series metadata for a given uuid
    return requests.get(orthanc_url+'/studies/'+uuid+'/series', verify=False).json()

def get_dicom_field(orthanc_url,input_df,field):
# get a specific field in the study metadata (e.g., 'StudyDate') for an input_df of accession numbers
# returns a df with a new column for the field-of-interest
    fields=[]
    found=0
    for index,row in input_df.iterrows():
        # get Orthanc UUID based on accession number
        access_num = str(row['accession_num'])
        uuids = get_uuids_from_accession(orthanc_url,access_num)
        if len(uuids) == 0: # no uuids found
            fields.append([' ',' ',' '])
        else:
            if len(uuids) > 1: # if more than one uuid for this accession
                field_list=[]
                for uuid in uuids:
                    info = get_study_metadata(orthanc_url,uuid)
                    field_list.append(info["MainDicomTags"][field])
                fields.append([access_num,uuids,field_list])
                found=1
            else:
                uuid = uuids[0]
                info = get_study_metadata(orthanc_url,uuid)
                field_value = info["MainDicomTags"][field]
                fields.append([access_num,uuid,field_value])
                found=1
    if found: # if there's at least found
        dates_df = pd.DataFrame(fields,columns=["accession_num","uuid",field])
        if field == 'StudyDate':
            dates_df[field] = dates_df[field].astype(str).apply(lambda x: x[0:4]+'-'+x[4:6]+'-'+x[6:8] if (x != 'nan') else x) # reformat 19990101 to 1999-01-01
        input_df['accession_num'] = input_df['accession_num'].astype('str')
        output_df = pd.merge(input_df,dates_df,on='accession_num',how='left')
        return output_df
    else:
        return input_df

# the following code is to upload a .zip to an Orthanc instance
#   modified from: https://hg.orthanc-server.com/orthanc/file/Orthanc-1.9.7/OrthancServer/Resources/Samples/ImportDicomFiles/OrthancImport.py
def IsJson(content):
    try:
        if (sys.version_info >= (3, 0)):
            json.loads(content.decode())
            return True
        else:
            json.loads(content)
            return True
    except:
        return False

def UploadBuffer(dicom,target_url):
    if IsJson(dicom):
        return
    r = requests.post('%s/instances' % target_url, verify=False, data = dicom) # this is the upload
    try:
        r.raise_for_status()
    except:
        return
    # info = r.json()
    # r2 = requests.get('%s/instances/%s/tags?short' % (target_url, info['ID']),
    #                   verify = False)
    # r2.raise_for_status()
    # tags = r2.json()
    # print('')
    # print('New imported study:')
    # print('  Orthanc ID of the patient: %s' % info['ParentPatient'])
    # print('  Orthanc ID of the study: %s' % info['ParentStudy'])
    # print('  DICOM Patient ID: %s' % (
    #     tags['0010,0020'] if '0010,0020' in tags else '(empty)'))
    # print('  DICOM Study Instance UID: %s' % (
    #     tags['0020,000d'] if '0020,000d' in tags else '(empty)'))
    # print('')

def UploadZip(path,target_url):
    print('Uncompressing ZIP archive: %s' % path)
    with zipfile.ZipFile(path, 'r') as zip:
        for item in zip.infolist():
            # WARNING - "item.is_dir()" would be better, but is not available in Python 2.7
            if item.file_size > 0:
                dicom = zip.read(item.filename)
                print('Uploading: %s (%dMB)' % (item.filename, len(dicom) / (1024 * 1024)))
                UploadBuffer(dicom,target_url)
