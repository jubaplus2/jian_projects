# -*- coding: utf-8 -*-
"""
Created on Wed May 25 16:00:04 2016

@author: Jubaplus
"""
import logging
logging.basicConfig(filename='personintablogs.log', level=logging.INFO)
logging.info('File0 PersonIntab')
logging.info('Started')
import datetime
logging.info(datetime.datetime.now())


Startdate=20150217
Enddate=20161009

finaldirct='/disk2/OTProject/MovieOT/personintab150217_161009.csv'

import datetime
import pandas as pd
import numpy as np

a=datetime.datetime.strptime(str(Startdate), '%Y%m%d').date()
b=6-a.weekday()
startdate=a+datetime.timedelta(days=b)
c=datetime.datetime.strptime(str(Enddate), '%Y%m%d').date()
d=6-c.weekday()
enddate=c+datetime.timedelta(days=d)
del(b,d,Startdate,Enddate)

directorypath='/root/SpencerNielsen/NielsenDirectoryServer.csv'
directory=pd.read_csv(directorypath)
directory['Week']=pd.to_datetime(directory['Week'])

names25=directory[(directory['Week']<=enddate)&(directory['Week']>=startdate)&(directory['Table']=='EHH')]
names25=names25['FilePath']

df=pd.DataFrame()
df2=pd.DataFrame()
type25=pd.DataFrame()
type26=pd.DataFrame()
for i in names25:
    df=pd.read_csv(i,sep='\t')
    if 'account1' in i:
        j=i[34:44].replace('_','')
    else:
        j=i[38:48].replace('_','')
    df.columns=['all']
    df['type']=df['all'].str[0:2]
    df2=df[df['type']=='26']
    df=df[df['type']=='25']
    df['HHID']=df['all'].str[13:20]
    df['Week']=df['all'].str[5:13]
    df=df[df['Week']==j]
    df['Mon']=df['all'].str[20:26]
    df['Tue']=df['all'].str[26:32]
    df['Wed']=df['all'].str[32:38]
    df['Thu']=df['all'].str[38:44]
    df['Fri']=df['all'].str[44:50]
    df['Sat']=df['all'].str[50:56]
    df['Sun']=df['all'].str[56:62]
    df=df[['HHID', 'Week', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat','Sun']]
    df=df.drop_duplicates()   
    type25=type25.append(df,ignore_index=True)
    df2['HHID']=df2['all'].str[13:20]
    df2['PersonID']=df2['all'].str[20:22]
    df2['Week']=df2['all'].str[5:13]
    df2=df2[df2['Week']==j]
    df2['Mon']=df2['all'].str[22:28]
    df2['Tue']=df2['all'].str[28:34]
    df2['Wed']=df2['all'].str[34:40]
    df2['Thu']=df2['all'].str[40:46]
    df2['Fri']=df2['all'].str[46:52]
    df2['Sat']=df2['all'].str[52:58]
    df2['Sun']=df2['all'].str[58:64]
    df2=df2[['HHID','PersonID','Week', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat','Sun']]
    df2=df2.drop_duplicates()   
    type26=type26.append(df2,ignore_index=True)
    logging.info(i)
    logging.info(j)
    
df=pd.DataFrame()
HHIntab=pd.DataFrame()
week=['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat','Sun']
number=[1,2,3,4,5,6,7]
for i,j in zip(week,number):
    df=type25[['HHID', 'Week',i]]
    df.columns=['HHID', 'Week','HHIntab']
    df['Weeknumber']=j
    df=df[df['HHIntab']!='000000']
    HHIntab=HHIntab.append(df,ignore_index=True)
    
HHIntab['HHIntab']=HHIntab['HHIntab'].astype(int)

df=pd.DataFrame()
PersonIntab=pd.DataFrame()
week=['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat','Sun']
number=[1,2,3,4,5,6,7]
for i,j in zip(week,number):
    df=type26[['HHID', 'Week','PersonID',i]]
    df.columns=['HHID', 'Week','PersonID','PersonIntab']
    df['Weeknumber']=j
    df=df[df['PersonIntab']!='000000']
    PersonIntab=PersonIntab.append(df,ignore_index=True)
    
PersonIntab['PersonIntab']=PersonIntab['PersonIntab'].astype(int)


HHIntab['ViewDate2']=pd.to_datetime(HHIntab['Week'])
HHIntab['Weeknumber']=HHIntab['Weeknumber']-7

HHIntab['ViewDate']=np.where(HHIntab['Weeknumber']==-1,(HHIntab['ViewDate2']+pd.DateOffset(-1)),
np.where(HHIntab['Weeknumber']==-2,(HHIntab['ViewDate2']+pd.DateOffset(-2)),
np.where(HHIntab['Weeknumber']==-3,(HHIntab['ViewDate2']+pd.DateOffset(-3)),
np.where(HHIntab['Weeknumber']==-4,(HHIntab['ViewDate2']+pd.DateOffset(-4)),
np.where(HHIntab['Weeknumber']==-5,(HHIntab['ViewDate2']+pd.DateOffset(-5)),
np.where(HHIntab['Weeknumber']==-6,(HHIntab['ViewDate2']+pd.DateOffset(-6)),
(HHIntab['ViewDate2'])))))))
HHIntab=HHIntab[['HHID', 'HHIntab','ViewDate']]
HHIntab=HHIntab[HHIntab['HHIntab']>0]
HHIntab=HHIntab.drop_duplicates(['HHID', 'ViewDate'])

PersonIntab['ViewDate2']=pd.to_datetime(PersonIntab['Week'])
PersonIntab['Weeknumber']=PersonIntab['Weeknumber']-7

PersonIntab['ViewDate']=np.where(PersonIntab['Weeknumber']==-1,(PersonIntab['ViewDate2']+pd.DateOffset(-1)),
np.where(PersonIntab['Weeknumber']==-2,(PersonIntab['ViewDate2']+pd.DateOffset(-2)),
np.where(PersonIntab['Weeknumber']==-3,(PersonIntab['ViewDate2']+pd.DateOffset(-3)),
np.where(PersonIntab['Weeknumber']==-4,(PersonIntab['ViewDate2']+pd.DateOffset(-4)),
np.where(PersonIntab['Weeknumber']==-5,(PersonIntab['ViewDate2']+pd.DateOffset(-5)),
np.where(PersonIntab['Weeknumber']==-6,(PersonIntab['ViewDate2']+pd.DateOffset(-6)),
(PersonIntab['ViewDate2'])))))))
PersonIntab=PersonIntab[['HHID', 'PersonID','PersonIntab','ViewDate']]
PersonIntab=PersonIntab.drop_duplicates(['HHID', 'PersonID','ViewDate'])
PersonIntab=pd.merge(PersonIntab,HHIntab,on=['HHID','ViewDate'])
PersonIntab=PersonIntab[['HHID', 'PersonID','ViewDate','PersonIntab','HHIntab']]
PersonIntab=PersonIntab[(PersonIntab['ViewDate']<=c)&(PersonIntab['ViewDate']>=a)]

PersonIntab.to_csv(finaldirct,index=False,dtype={'HHID':'object','PersonID':'object'})
logging.info(PersonIntab.groupby('ViewDate').count())


del(startdate,enddate,finaldirct,type26,type25,HHIntab,PersonIntab,df,number,names25)
import gc
gc.collect()

logging.info('Finished')
logging.info(datetime.datetime.now())