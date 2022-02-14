import json
import requests
import os
import zipfile
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_orthanc_url(flag):
    if flag == 'AWS':
        cred = 
        ip = 
        port = 
    if flag == 'HPC':
        cred = 
        ip = 
        port = 
    if flag == 'K8':
        cred=
        ip=
        port=
    return 'http://'+cred+'@'+ip+':'+port

def all_study_uuids(orthanc_url):
# get a list of all study uuids for a given instance
    return requests.get(orthanc_url+'/studies/', verify=False).json()

def all_patient_uuids(orthanc_url):
    return requests.get(orthanc_url+'/patients/', verify=False).json()

def get_uuids_from_accession(orthanc_url,accession):
# requires orthanc_url
    data = {'Level':'Study',
            'Query':{'AccessionNumber':str(accession)} }
    data_json = json.dumps(data)
    resp = requests.post(orthanc_url+'/tools/find', data=data_json, verify=False)
    return resp.json()

def get_uuids_from_mrn(orthanc_url,mrn):
# requires orthanc_url
    data = {'Level':'Study',
            'Query':{'PatientID':str(mrn)} }
    data_json = json.dumps(data)
    resp = requests.post(orthanc_url+'/tools/find', data=data_json, verify=False)
    return resp.json()

def get_uuids(orthanc_url,accession_df,in_type):
    out_list = []
    missing_list=[]
    df_col=[]
    for ind,row in accession_df.iterrows():
        if in_type=='accession':
            unique_id = row['accession_num']
            uuids = get_uuids_from_accession(orthanc_url,unique_id)
        elif in_type=='mrn':
            unique_id = row['MRN']
            uuids = get_uuids_from_mrn(orthanc_url,unique_id)
        if uuids:
            df_col.append(uuids)
            out_list = out_list + uuids
        else:
            df_col.append([' '])
            missing_list.append(unique_id)
    accession_df['uuids']=df_col
    return out_list,missing_list,accession_df

def get_study_metadata(orthanc_url,uuid):
# requires orthanc_url
    return requests.get(orthanc_url+'/studies/'+uuid+'/', verify=False).json()

def get_series_metadata(orthanc_url,uuid):
    return requests.get(orthanc_url+'/studies/'+uuid+'/series', verify=False).json()

def get_patient_metadata(orthanc_url,uuid):
    return requests.get(orthanc_url+'/patients/'+uuid+'/', verify=False).json()

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

def all_instance_mrns(orthanc_url):
# get a list of mrns for all studies on a given instance
#   this takes a lil while to run
    patient_uuids = all_patient_uuids(orthanc_url)
    all_mrns=[]
    for uuid in patient_uuids:
        mrn = get_patient_metadata(orthanc_url,uuid)['MainDicomTags']['PatientID']
        if mrn not in all_mrns:
            all_mrns.append(mrn)
    return all_mrns

def all_instance_accessions(orthanc_url):
# get a list of mrns for all studies on a given instance
#   this takes a lil while to run
    study_uuids = all_study_uuids(orthanc_url)
    all_accessions=[]
    for uuid in study_uuids:
        accession_num = get_study_metadata(orthanc_url,uuid)['MainDicomTags']['AccessionNumber']
        if accession_num not in all_accessions:
            all_accessions.append(accession_num)
    return all_accessions

def compare_s3_orthanc(orthanc_url,s3_path):
# compares uuids between processed data in S3 bucket & Orthanc
#   returns [list of uuids to process] if not matching & more data on Orthanc (trigger for pipeline)
#   returns 0 if matching, or not matching & more data on S3

    ## create local text file with list of files on s3 & load the list
    os.system('aws s3 ls '+s3_path+'/DICOMs/backup/ | awk '"'{print $4}'"' > s3_files.txt')
    with open('s3_files.txt', mode="r", encoding="utf-8") as f:
        s3_fns = f.readlines()

    ## strip file names to get list of uuids
    s3_uuids = []
    for file in s3_fns:
        fn = file.strip('.zip\n')
        s3_uuids.append(fn)

    ## get list of uuids on Orthanc
    orthanc_uuids = all_study_uuids(orthanc_url)

    # compare the 2 lists, if more files on Orthanc, return list of uuids for the next process
    out_list = list(set(orthanc_uuids)-set(s3_uuids))
    if out_list:
        return out_list
    else:
        return 0

def download_study(orthanc_url,uuid,output_path):
# download a single study archive to local disk based on uuid
    os.system('curl -s -k '+orthanc_url+'/studies/'+uuid+'/archive > '+output_path)

def download_unpack_copy(orthanc_url,s3_path,uuids,data_dir):
# downloads data from Orthanc based on list of uuids as "{uuid}.zip"
#   copies .zip to s3/backup
#   unpacks .zip & deletes it locally
# restricted to "MR" modality studies only
    processed=[]
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    for uuid in uuids:
        info = get_series_metadata(orthanc_url,uuid)
        modality = info[0]['MainDicomTags']['Modality']
        output_fn = uuid+'.zip'
        output_path = data_dir+output_fn
        ## download this archive
        download_study(orthanc_url,uuid,output_path)
        ## copy backup to s3
        os.system('aws s3 cp '+output_path+' '+s3_path+'DICOMs/backup/')
        ## unpack it
        with zipfile.ZipFile(output_path, 'r') as zip_ref:
            zip_ref.extractall(data_dir)
        ## delete local zip
        os.remove(output_path)
        processed.append(uuid)
    return processed

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

def upload_dicoms(path,target_url):
    print('Uploading DICOMs to Orthanc at '+target_url)
    for root,dirs,files in os.walk(path):
        for file in files:
            f = open(os.path.join(root,file), 'rb')
            content = f.read()
            UploadBuffer(content,target_url)
            f.close()
