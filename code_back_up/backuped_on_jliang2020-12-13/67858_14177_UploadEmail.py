import Function_UploadEmail as uploademail
import pandas as pd
import paramiko


folderpath = '/mnt/drv5/Ubank/Suppression/' # change here

##Facebook 
ACCESS_TOKEN = ''
accountid = 'act_'

#### upload to Facebook
import os
filelist = os.listdir(folderpath)
for filename in filelist:
    fbname = filename[0:-4]
    filename = folderpath + filename
    df = pd.read_csv(filename)
    uploademail.creatfbaudience(fbname,df,ACCESS_TOKEN,accountid)
    print(fbname)

#### upload to Live Ramp
liverampfolder = '/uploads/mediastorm_onboarding_umpqua/'
transport = uploademail.build_transport()
sftp = paramiko.SFTPClient.from_transport(transport)
for filename in filelist:
    sftp.put(folderpath + filename,
             liverampfolder + filename)
sftp.close()
transport.close()

#### upload to Adwords
# change config in googlead_sk.yaml file
from googleads import adwords
auth_file_path = './googleads_sk.yaml'
email_path = '/mnt/drv5/Ubank/DnB Uploads Small Business/Encode_2019.11.14_MediaStorm_SB_Suppression_List.csv'

adwords_client = adwords.AdWordsClient.LoadFromStorage(auth_file_path)
uploademail.main(adwords_client, email_path)