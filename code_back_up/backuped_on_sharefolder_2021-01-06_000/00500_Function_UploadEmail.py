from multiprocessing import Pool #ensure access to the code libraries for line 5-13
import requests
from json import dumps
import time
import csv
from hashlib import sha1
import hmac
import base64
import sys
import pandas as pd
import numpy as np
from facebookads.adobjects.customaudience import CustomAudience
from facebookads.adobjects.adaccount import AdAccount
from facebookads.api import FacebookAdsApi
APP_ID = '1954772121206438'

def creatfbaudience(fbname,df,ACCESS_TOKEN,accountid):
    FacebookAdsApi.init(access_token=ACCESS_TOKEN, api_version='v4.0')
    myAccount = AdAccount(accountid)
    fields = [
    ]
    params = {
      'name': fbname,
      'subtype': 'CUSTOM',
      #'description': 'People who purchased on my website',
      'customer_file_source': 'USER_PROVIDED_ONLY',
    }
    fbname = myAccount.create_custom_audience(
      fields=fields,
      params=params,
    )['id']
    
    listlen = df.shape[0]
    requestUrl = 'https://graph.facebook.com/v4.0/'+ fbname + '/users'
    for i in range(int(np.ceil(listlen/50000))):
        payload = {}
        payload['schema'] = ['EMAIL'] #  change here if would like to upload different infor:  ['FN','LN','EMAIL'] 
        starti = i*50000
        if (i+1)*50000<listlen:
            endi = (i+1)*50000
        else:
            endi = listlen
        payload_data = list()
        data_df = df.iloc[starti:endi,:]
        data_df = data_df.dropna() #check
        for row in data_df.itertuples():
            data = list()
            data.append(row.email) #  change here to upload different infor:  data.append(row.fn)   
            payload_data.append(data)
        payload['data'] = payload_data
        params = {}
        params ={'payload':payload} 
        CustomAudience(fbname).create_user(
                      fields=fields,
                      params=params,
        )
    '''
    audience = CustomAudience(fbname)
    column_name = [i for i in df.columns if 'email' in i]
    if len(column_name) >1:
        emails = list(df[column_name[0]].append(df[column_name[1]]))
        emails = [i for i in emails if str(i) != 'nan']
    else:
        emails = list(df[column_name[0]])
    listlen = len(emails)
    '''
    return 'done'


import paramiko
host = "sftp.liveramp.com"
port = 22
password = "Biglots2018!"
username = "mediastorm"
def build_transport():
    transport = paramiko.Transport((host, port))
    transport.default_window_size=paramiko.common.MAX_WINDOW_SIZE
    transport.packetizer.REKEY_BYTES = pow(2, 40)
    transport.packetizer.REKEY_PACKETS = pow(2, 40)
    transport.connect(username = username, password=password)
    return transport

import hashlib
import uuid
# Import appropriate modules from the client library.
from googleads import adwords
import os

auth_file_path = './googleads_sk.yaml'

def main(client, email_path):
  # Initialize appropriate services.
    user_list_service = client.GetService('AdwordsUserListService', 'v201809')
    user_list = {
        'xsi_type': 'CrmBasedUserList',
        'name': email_path.split("/")[-1][:-4] ,
        'description': 'A list of customers that originated from email addresses',
        # CRM-based user lists can use a membershipLifeSpan of 10000 to indicate
        # unlimited; otherwise normal values apply.
        'membershipLifeSpan': 10000,
        'uploadKeyType': 'CONTACT_INFO'
     }
    # Create an operation to add the user list.
    operations = [{
        'operator': 'ADD',
        'operand': user_list
    }]
    result = user_list_service.mutate(operations)
    user_list_id = result['value'][0]['id']

    df = pd.read_csv(email_path, chunksize=3000)
    for i in df:
        mail_col = [k for k in i.columns if 'email' in k.lower()][0]
        new_col = ['email' if h == mail_col else h for h in i.columns.tolist()]
        i.columns = new_col
        emails = i['email']
        emails = set([i for i in emails if str(i) != 'nan'])
        members = [{'hashedEmail': email} for email in emails]
        upload_emails_google(user_list_service, user_list_id, members)

def upload_emails_google(service, user_list_id, email_list):
    mutate_members_operation = {
      'operand': {
          'userListId': user_list_id,
          'membersList': email_list
      },
      'operator': 'ADD'
    }
    response = service.mutateMembers([mutate_members_operation])
    if 'userLists' in response:
        for user_list in response['userLists']:
            print('Uploaded %i items to user list with name "%s" and ID "%d"'
                % (len(email_list), user_list['name'], user_list['id']))