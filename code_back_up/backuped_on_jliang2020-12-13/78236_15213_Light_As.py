# -*- coding: utf-8 -*-
"""
Created on Mon May 23 10:30:00 2016

@author: Jubaplus
"""
import logging
logging.basicConfig(filename='Light_As.log', level=logging.INFO)
logging.info('File1 Quads')
logging.info('Started')
import datetime
logging.info(datetime.datetime.now())

###### make sure the directory file is up to date
####put startdate and enddate for the project here:
Startdate=20170114
Enddate=20180114
qmins = 100

##### update Cable ID (WeTV: '007516',SUND: '007602',STARZ: '009157', WGNA: '6788')
Cable_ID=''
Program_ID=['0241330','0415605','0284260','0345570','0365623','0346627']
##### update the outputs path and location
finalfiledic='/home/nielsen/MicroSegment/Outputs/Quads/Light_As.csv'

#####code starts here, DO NOT make any changes
directorypath='/home/nielsen/Nielsen_Directory_Server/NielsenDirectoryServer.csv'
import datetime
a=datetime.datetime.strptime(str(Startdate), '%Y%m%d').date()
b=6-a.weekday()
startdate=a+datetime.timedelta(days=b)
c=datetime.datetime.strptime(str(Enddate), '%Y%m%d').date()
d=6-c.weekday()
enddate=c+datetime.timedelta(days=d)
del(a,b,c,d)

import pandas as pd
directory=pd.read_csv(directorypath)
directory['Week']=pd.to_datetime(directory['Week'])

names36=directory[(directory['Week']<=enddate)&(directory['Week']>=startdate)&(directory['Table'].isin(['C36']))]
names36=names36['FilePath']

names30=directory[(directory['Week']<=enddate)&(directory['Week']>=startdate)&(directory['Table'].isin(['C30']))]
names30=names30['FilePath']

df=pd.DataFrame()
type36=pd.DataFrame()
for i in names36:
    df=pd.read_csv(i,sep='\t')
    df.columns=['all']
    df['type']=df['all'].str[0:2]
    df=df[df['type']=='36']
    df['HHID']=df['all'].str[29:36]
    df['CableID']=df['all'].str[3:9]
    #df=df[df['CableID']==Cable_ID]
    df['ViewDate']=df['all'].str[68:76]
    df['PersonID']=df['all'].str[36:38]
    df=df[df['PersonID']!='00']
    df['Programcode']=df['all'].str[9:16]
    df=df[df['Programcode'].isin(Program_ID)]
    df['TelecastN']=df['all'].str[16:23]
    df['ViewStartTime']=df['all'].str[38:44]
    df['ViewEndTime']=df['all'].str[44:50]   
    df['ShiftedViewCode']=df['all'].str[76:77]
    df['VCRCode']=df['all'].str[55:56]
    df['PlatformCode']=df['all'].str[114:116]
    df['VODIndicator']=df['all'].str[116:118]
    df['DelayMins']=df['all'].str[109:114]

    #JAY
    df['Age'] = df['all'].str[56:59].astype(int)
    df['Sex Code'] = df['all'].str[59:60]
    
    type36=type36.append(df,ignore_index=True)
#Jay 
type36.to_csv('type36.csv', index = False)

df=pd.DataFrame()
type30=pd.DataFrame()
for i in names30:
    df=pd.read_csv(i,sep='\t')
    df.columns=['all']
    df['type']=df['all'].str[0:2]
    df=df[df['type']=='30']
    df['ProgramDate']=df['all'].str[39:47]
    df['CableID']=df['all'].str[8:14]
    #df=df[df['CableID']==Cable_ID]
    type30=type30.append(df,ignore_index=True)
  
type30['CableName']=type30['all'].str[3:8]
type30['Programcode']=type30['all'].str[14:21]
type30['ProgramName']=type30['all'].str[55:80]
type30['TelecastN']=type30['all'].str[21:28]
type30['StartTime']=type30['all'].str[88:92].astype(int)
type30['EndTime']=type30['all'].str[94:98].astype(int)
type30['Date2']=type30['ProgramDate'].astype('int')
type30['Weekday']=pd.to_datetime(type30['ProgramDate']).dt.dayofweek
type30=type30[(type30['Date2']>=Startdate)&(type30['Date2']<=Enddate)]
#type30=type30[(((type30['StartTime']>2000)&(type30['StartTime']<2300))|((type30['EndTime']>2000)&(type30['EndTime']<2300)))]

#Ellen
#if Cable_ID=='007516':
#    type30=type30[type30.Weekday.isin([3,4])]

type36['Date2']=type36['ViewDate'].astype('int')
type36=type36[(type36['Date2']>=Startdate)&(type36['Date2']<=Enddate)]
#Jay
type36=type36[(((type36['Age'] >= 25) & (type36['Age'] <= 54)))]

allfile=pd.merge(type36,type30,on=['CableID','Programcode','TelecastN'])
allfile['ViewDate']=pd.to_datetime(allfile['ViewDate'])

allfile=allfile[['HHID', 'CableID', 'ViewDate', 'PersonID',
       'Programcode', 'TelecastN', 'ViewStartTime', 'ViewEndTime',
       'ShiftedViewCode', 'VCRCode', 'PlatformCode', 'VODIndicator',
       'DelayMins', 'ProgramDate', 'CableName',
       'ProgramName', 'StartTime', 'EndTime',
        #Jay
        'Sex Code', 'Age']]

allfile=allfile.drop_duplicates()
allfile['ViewStartTime']=allfile['ViewStartTime'].astype(int)
allfile['ViewEndTime']=allfile['ViewEndTime'].astype(int)
allfile['mins']=allfile['ViewEndTime']-allfile['ViewStartTime']

#jay
#allfile.to_csv('/disk2/Projects/SwampPeopleOutput/check.csv', index = False)

totalmins=allfile[['HHID', 'PersonID','mins']]
totalmins=totalmins.groupby(['HHID', 'PersonID']).sum()
totalmins.reset_index(inplace=True)  
totalmins.columns=['HHID', 'PersonID', 'Totalmins']
totalmins = totalmins[totalmins['Totalmins']>=qmins]
totalmins['Quads']=1
totalmins.to_csv(finalfiledic,index=False)

logging.info(allfile[['PersonID','ViewDate']].groupby('ViewDate').count())

del (i,directorypath,startdate,enddate,Startdate,Enddate,Cable_ID,finalfiledic,directory,names36,names30,type36,type30,df,totalmins,allfile)
import gc
gc.collect()

logging.info('Finished')
logging.info(datetime.datetime.now())
