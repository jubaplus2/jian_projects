# -*- coding: utf-8 -*-
"""
Created on Mon May 23 10:30:00 2016

@author: Jubaplus
"""
import logging
logging.basicConfig(filename='mamajuneviewership.log', level=logging.INFO)
logging.info('File1 Viewershipclassification')
logging.info('Started')
import datetime
logging.info(datetime.datetime.now())

###### make sure the directory file is up to date
####put startdate and enddate for the project here:
Startdate=20160813
Enddate=20161113

##### update the outputs path and location
finalfiledic='/disk2/Projects/AudienceSizing/MamaJune/ViewershipClassification.csv'



#####code starts here, DO NOT make any changes
directorypath='/root/SpencerNielsen/NielsenDirectoryServer.csv'

import datetime
a=datetime.datetime.strptime(str(Startdate), '%Y%m%d').date()
b=6-a.weekday()
startdate=a+datetime.timedelta(days=b)
c=datetime.datetime.strptime(str(Enddate), '%Y%m%d').date()
d=6-c.weekday()
enddate=c+datetime.timedelta(days=d)
del(a,b,c,d)

import pandas as pd
import numpy as np

directory=pd.read_csv(directorypath)
directory['Week']=pd.to_datetime(directory['Week'])

names36=directory[(directory['Week']<=enddate)&(directory['Week']>=startdate)&((directory['Table']=='C36')|(directory['Table']=='B36'))]
names36=names36['FilePath']


df=pd.DataFrame()
type36=pd.DataFrame()
for i in names36:
    df=pd.read_csv(i,sep='\t')
    df.columns=['all']
    df['type']=df['all'].str[0:2]
    df=df[df['type']=='36']
    df['HHID']=df['all'].str[29:36]
    df['ViewDate']=df['all'].str[68:76].astype('int')
    df=df[(df['ViewDate']>=Startdate)&(df['ViewDate']<=Enddate)]
    df['PersonID']=df['all'].str[36:38]
    df=df[df['PersonID']!='00']
    df['ShiftedViewCode']=df['all'].str[76:77]
    df['ShiftedViewCode']=np.where(df['ShiftedViewCode']=="1",1,0)
    df['Programcode']=df['all'].str[9:16]
    df['TelecastN']=df['all'].str[16:23]
    df['count']=1
    df=df[['HHID','PersonID','Programcode','TelecastN','ShiftedViewCode','count']]
    df=df.groupby(['HHID','PersonID','Programcode','TelecastN']).sum()
    df.reset_index(inplace=True) 
    type36=type36.append(df,ignore_index=True)
    logging.info(i)

type36=type36.groupby(['HHID','PersonID','Programcode','TelecastN']).sum()
type36.reset_index(inplace=True) 
type36['ShiftedViewCode']=type36['ShiftedViewCode']/type36['count']
type36['ShiftedViewCode']=np.where(type36['ShiftedViewCode']>=0.5,1,0)

shiftview=type36[['HHID','PersonID','ShiftedViewCode']]
shiftview=shiftview.groupby(['HHID','PersonID']).mean()
shiftview.reset_index(inplace=True) 
shiftview['vodsegment']=np.where(shiftview['ShiftedViewCode']==1,"LiveOnly","Bothandshift")
shiftview=shiftview[['HHID','PersonID','vodsegment']]

shiftviewcount=type36[type36['ShiftedViewCode']!=1]
shiftviewcount=shiftviewcount[['HHID','PersonID','ShiftedViewCode']]
shiftviewcount=shiftviewcount.groupby(['HHID','PersonID']).count()
shiftviewcount.reset_index(inplace=True)
shiftviewcount['ShiftedViewCode'].describe()


shiftviewcount['countvod']=np.where(shiftviewcount['ShiftedViewCode']<=5,'count1-5',
       np.where(shiftviewcount['ShiftedViewCode']<=50,'count6-50','count51+'))
shiftviewcount=shiftviewcount[['HHID','PersonID','countvod']]

shiftview=pd.merge(shiftview,shiftviewcount,on=['HHID','PersonID'],how='outer')
shiftview=shiftview.fillna(0)
logging.info(shiftview.groupby('vodsegment').count())
logging.info(shiftview.groupby('countvod').count())

shiftview.to_csv(finalfiledic,index=False)

del (shiftviewcount,i,directorypath,startdate,enddate,Startdate,Enddate,shiftview,finalfiledic,directory,names36,type36,df)
import gc
gc.collect()
gc.collect()
logging.info(datetime.datetime.now())
logging.info('Finished')