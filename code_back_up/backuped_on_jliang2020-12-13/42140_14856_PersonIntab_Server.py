# -*- coding: utf-8 -*-
"""
Created on Wed May 25 16:00:04 2016

@author: Jubaplus
"""
import logging
logging.basicConfig(filename='audienceszinglogs.log', level=logging.INFO)
logging.info('File0 PersonIntab')
logging.info('Started')
import datetime
logging.info(datetime.datetime.now())

import pandas as pd
import numpy as np

startdate='2016-05-08'
enddate='2016-08-10'
finaldirct='/disk2/Projects/AudienceSizing/type26_508_810.csv'

names25=[
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_05_08_EHH_R0/N160510.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_05_15_EHH_R0/N160520.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_05_22_EHH_R0/N160530.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_05_29_EHH_R0/N160540.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_06_05_EHH_R0/N160550.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_06_12_EHH_R0/N160610.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_06_19_EHH_R0/N160620.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_06_26_EHH_R0/N160630.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_07_03_EHH_R0/N160640.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_07_10_EHH_R0/N160710.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_07_17_EHH_R0/N160720.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_07_24_EHH_R0/N160730.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_07_31_EHH_R0/N160740.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_08_07_EHH_R0/N160750.EHH',
'/disk2/Projects/NielsenData/NPM_ARD_W_2016_08_14_EHH_R0/N160810.EHH'
]
week=[
'20160508',
'20160515',
'20160522',
'20160529',
'20160605',
'20160612',
'20160619',
'20160626',
'20160703',
'20160710',
'20160717',
'20160724',
'20160731',
'20160807',
'20160814']

df=pd.DataFrame()
df2=pd.DataFrame()
type25=pd.DataFrame()
type26=pd.DataFrame()
for i,j in zip(names25,week):
    df=pd.read_csv(i,sep='\t')
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
PersonIntab=PersonIntab[(PersonIntab['ViewDate']<=enddate)&(PersonIntab['ViewDate']>=startdate)]

PersonIntab.to_csv(finaldirct,index=False,dtype={'HHID':'object','PersonID':'object'})
logging.info(PersonIntab.groupby('ViewDate').count())


del(startdate,enddate,finaldirct,type26,type25,HHIntab,PersonIntab,df,week,number,names25)
import gc
gc.collect()

logging.info('Finished')
logging.info(datetime.datetime.now())