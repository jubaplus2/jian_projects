# -*- coding: utf-8 -*-
"""
Created on Mon Jun  6 15:02:11 2016

@author: Jubaplus
"""


###### make sure the directory file is up to date
directorypath='/Users/jubaplus1/Downloads/NielsenDirectoryServer.csv'
personintab='/Users/jubaplus1/Desktop/Open Road/The Promise/Sizing/personintab_150101_170108.csv'
####put startdate and enddate for the project here:
Startdate=20161008
Enddate=20170108

###outputpath
outputpath='/Users/jubaplus1/Desktop/wetvL3M.csv'


##### For Quads update Cable ID (WeTV: '007516',SUND: '007602',STARZ: '009157', WGNA: '6788')
Cable_ID='007516'
####if do not want prime time for quads, update below varaible to 'F'
primetimeindicator='F'

###set age
lowage=2
highage=99

###set gender
gender=['F','M']

import pandas as pd
import numpy as np

###download this file from server
PersonIntab=pd.read_csv(personintab)
'''
PersonIntab['ViewDate2']=pd.to_datetime(PersonIntab['Week'])
PersonIntab.groupby('Weeknumber').count()
PersonIntab['Weeknumber']=PersonIntab['Weeknumber']-7

PersonIntab['ViewDate']=np.where(PersonIntab['Weeknumber']==-1,(PersonIntab['ViewDate2']+pd.DateOffset(-1)),
np.where(PersonIntab['Weeknumber']==-2,(PersonIntab['ViewDate2']+pd.DateOffset(-2)),
np.where(PersonIntab['Weeknumber']==-3,(PersonIntab['ViewDate2']+pd.DateOffset(-3)),
np.where(PersonIntab['Weeknumber']==-4,(PersonIntab['ViewDate2']+pd.DateOffset(-4)),
np.where(PersonIntab['Weeknumber']==-5,(PersonIntab['ViewDate2']+pd.DateOffset(-5)),
np.where(PersonIntab['Weeknumber']==-6,(PersonIntab['ViewDate2']+pd.DateOffset(-6)),
(PersonIntab['ViewDate2'])))))))
'''
PersonIntab=PersonIntab[['HHID', 'PersonID','PersonIntab','ViewDate']]
PersonIntab['ViewDate']=pd.to_datetime(PersonIntab['ViewDate'])
PersonIntab=PersonIntab.drop_duplicates(['HHID', 'PersonID','ViewDate'])

import datetime
a=datetime.datetime.strptime(str(Startdate), '%Y%m%d').date()
b=6-a.weekday()
startdate=a+datetime.timedelta(days=b)
c=datetime.datetime.strptime(str(Enddate), '%Y%m%d').date()
d=6-c.weekday()
enddate=c+datetime.timedelta(days=d)

PersonIntab2=PersonIntab[(PersonIntab['ViewDate']>=a)&(PersonIntab['ViewDate']<=c)]
del(a,b,c,d)


PersonIntab2=PersonIntab2.groupby(['HHID','PersonID']).min()
PersonIntab2.reset_index(inplace=True)
#PersonIntab2.to_csv('/Users/jubaplus/Desktop/HHintab2014.csv',index=False)

directory=pd.read_csv(directorypath)
directory['Week']=pd.to_datetime(directory['Week'])

names36=directory[(directory['Week']<=enddate)&(directory['Week']>=startdate)&(directory['Table']=='C36')]
names36=names36['FilePath']

names30=directory[(directory['Week']<=enddate)&(directory['Week']>=startdate)&(directory['Table']=='C30')]
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
    df=df[df['CableID']==Cable_ID]
    df['PersonID']=df['all'].str[36:38]
    df=df[df['PersonID']!='00']
    df['Age']=df['all'].str[56:59].astype(int)
    df=df[(df['Age']>=lowage)&(df['Age']<=highage)]
    df['Gender']=df['all'].str[59:60]
    df=df[df.Gender.isin(gender)]
    df['Programcode']=df['all'].str[9:16]
    df['TelecastN']=df['all'].str[16:23]
    df['DelayMins']=df['all'].str[109:114]    
    type36=type36.append(df,ignore_index=True)
 
   
df=pd.DataFrame()
type30=pd.DataFrame()
for i in names30:
    df=pd.read_csv(i,sep='\t')
    df.columns=['all']
    df['type']=df['all'].str[0:2]
    df=df[df['type']=='30']
    df['ProgramDate']=df['all'].str[39:47]
    df['CableID']=df['all'].str[8:14]
    df=df[df['CableID']==Cable_ID]
    type30=type30.append(df,ignore_index=True)
  
type30['Programcode']=type30['all'].str[14:21]
type30['TelecastN']=type30['all'].str[21:28]
type30['StartTime']=type30['all'].str[88:92].astype(int)
type30['EndTime']=type30['all'].str[94:98].astype(int)
type30['Date2']=type30['ProgramDate'].astype('int')
type30=type30[(type30['Date2']>=Startdate)&(type30['Date2']<=Enddate)]

if primetimeindicator=='T':
   type30=type30[(((type30['StartTime']>2000)&(type30['StartTime']<2300))|((type30['EndTime']>2000)&(type30['EndTime']<2300)))] 
if Cable_ID=='007516':
    type30['Weekday']=pd.to_datetime(type30['ProgramDate']).dt.dayofweek
    type30=type30[type30.Weekday.isin([3,4])]


STARZviewers=pd.merge(type36,type30,on=['CableID','Programcode','TelecastN'])

STARZviewers=STARZviewers[['HHID','PersonID','Age','Gender']]
STARZviewers=STARZviewers.drop_duplicates(['HHID','PersonID'])
STARZviewers['HHID']=STARZviewers['HHID'].astype(int)
STARZviewers['PersonID']=STARZviewers['PersonID'].astype(int)

df=pd.merge(STARZviewers,PersonIntab2,on=['HHID','PersonID'])

df.to_csv(outputpath,index=False)












