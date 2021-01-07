# -*- coding: utf-8 -*-
"""
Created on Tue Aug 16 10:47:46 2016

@author: Jubaplus
"""

####put startdate and enddate for the project here:
Startdate=20160717
Enddate=20160731

##### update Cable ID (WeTV: '007516',SUND: '007602',STARZ: '009157', WGNA: '6788')
Cable_ID=['009157']

##### update the final file path and location
directorypath='/Users/Jubaplus/Documents/R Scripts/Post Analysis/Nielsen Data/NielsenDirectory.csv'
nielsen30filedic='/Users/Jubaplus/Desktop/type30cable.csv'



#####code starts here, DO NOT make any changes
import datetime
a=datetime.datetime.strptime(str(Startdate), '%Y%m%d').date()
b=6-a.weekday()
startdate=a+datetime.timedelta(days=b)
c=datetime.datetime.strptime(str(Enddate), '%Y%m%d').date()
d=6-c.weekday()
enddate=c+datetime.timedelta(days=d)

import pandas as pd
directory=pd.read_csv(directorypath)
directory['Week']=pd.to_datetime(directory['Week'])

names30=directory[(directory['Week']<=enddate)&(directory['Week']>=startdate)&(directory['Table']=='C30')]
names30=names30['FilePath']

df=pd.DataFrame()
type30=pd.DataFrame()
for i in names30:
    df=pd.read_csv(i,sep='\t')
    df.columns=['all']
    df['type']=df['all'].str[0:2]
    df=df[df['type']=='30']
    df['ProgramDate']=df['all'].str[39:47]
    df['CableID']=df['all'].str[8:14]
    type30=type30.append(df,ignore_index=True)
  

type30=type30[type30.CableID.isin(Cable_ID)]
type30['CableName']=type30['all'].str[3:8]
type30['Programcode']=type30['all'].str[14:21]
type30['ProgramName']=type30['all'].str[55:80]
type30['TelecastN']=type30['all'].str[21:28].astype(int)
type30['StartTime']=type30['all'].str[88:92]
type30['EpisodeName']=type30['all'].str[130:170]
type30['Date2']=type30['ProgramDate'].astype(int)
type30=type30[(type30['Date2']>=Startdate)&(type30['Date2']<=Enddate)]
type30.to_csv(nielsen30filedic,index=False)  
