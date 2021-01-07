# -*- coding: utf-8 -*-
"""
Created on Wed Jun 28 12:20:23 2017

@author: Jubaplus
"""

import logging
import datetime
import pandas as pd
import numpy as np

coreclusters = [1,2,4,7,8,9,10]
occaclusters = [3,6]
Startdate = 20160620
Enddate = 20170609
idpath = '/home/nielsen/DMA_AND_MVPD/DanceMom_Hispanic/DanceMom_DMA_MVPD_file.csv'
outpath = '/home/nielsen/DMA_AND_MVPD/DanceMom_Hispanic/Mapped.csv'

########

def demographics(Startdate,Enddate):
    Startdate=datetime.datetime.strptime(str(Startdate), '%Y%m%d').date()
    startdate=Startdate+datetime.timedelta(days=(6-Startdate.weekday()))
    Enddate=datetime.datetime.strptime(str(Enddate), '%Y%m%d').date()
    enddate=Enddate+datetime.timedelta(days=(6-Enddate.weekday()))
    directory=pd.read_csv('/home/nielsen/Nielsen_Directory_Server/NielsenDirectoryServer.csv')
    directory['Week']=pd.to_datetime(directory['Week'])
    directory=directory.sort_values('Week',ascending=0)
    names14=directory[(directory['Week']<=enddate)&(directory['Week']>=startdate)&(directory['Table']=='HHC')]
    names14=names14['FilePath']
    names65=directory[(directory['Week']<=enddate)&(directory['Week']>=startdate)&(directory['Table']=='MVD')]
    names65=names65['FilePath']
    type14=pd.DataFrame()
    for i in names14:
        df=pd.read_csv(i,sep='\t')
        df.columns=['all']
        df['type']=df['all'].str[0:2]
        df=df[df['type']=='14']
        df['HHID']=df['all'].str[13:20]
        df['LPMFlag']=df['all'].str[64:65]
        df['LPMCode']=df['all'].str[65:68]
        df=df.drop_duplicates()   
        type14=type14.append(df,ignore_index=True)
        logging.info(i)    
    type14=type14[['HHID','LPMFlag','LPMCode']]
    type14=type14.drop_duplicates('HHID')
    df=pd.DataFrame()
    type65=pd.DataFrame()
    for i in names65:
        df=pd.read_csv(i,sep='\t')
        df.columns=['all']
        df['type']=df['all'].str[0:2]
        df=df[df['type']=='65']
        df['HHID']=df['all'].str[13:20]
        df['starteffetivedate']=df['all'].str[20:28].astype('int')       
        df['ATT']=df['all'].str[37:38]
        df['Brighthouse']=df['all'].str[38:39]
        df['Cablevision']=df['all'].str[39:40]
        df['Charter']=df['all'].str[40:41]
        df['Comcast']=df['all'].str[41:42]
        df['Cox']=df['all'].str[42:43]
        df['Time Warner']=df['all'].str[43:44]
        df['Verizon']=df['all'].str[44:45]
        df['DirecTV']=df['all'].str[45:46]
        df['Dish Network']=df['all'].str[46:47]
        df=df.drop_duplicates()   
        type65=type65.append(df,ignore_index=True)
        logging.info(i)  
    type65=type65.sort_values('starteffetivedate',ascending=0)
    type65=type65.drop_duplicates('HHID')
    df=pd.DataFrame()
    type65v2=pd.DataFrame()
    MVPD=['ATT', 'Brighthouse', 'Cablevision', 'Charter', 'Comcast', 'Cox','Time Warner','Verizon','DirecTV','Dish Network']
    for i in MVPD:
        df=type65[['HHID', i]]
        df.columns=['HHID', 'MVPD']
        df=df[df['MVPD']=='Y']
        df['MVPD']=i
        type65v2=type65v2.append(df,ignore_index=True)
    alldemo=pd.merge(type14,type65v2,on='HHID',how='outer')
    alldemo['MVPD'].fillna('Other',inplace=True)
    alldemo=alldemo.drop_duplicates(['HHID'])
    return alldemo

demo = demographics(Startdate,Enddate)
HHIDfile = pd.read_csv(idpath)
HHIDfile['HHID'] = HHIDfile['HHID_PersonID'].str[0:7]
HHIDfile = pd.merge(HHIDfile, demo, on = 'HHID', how ='left')
HHIDfile['FinalCluster'] = np.where(HHIDfile['Cluster'].isin(coreclusters),'core',
                           np.where(HHIDfile['Cluster'].isin(occaclusters),'occasional',
                                    'balance'))
HHIDfile.to_csv(outpath,index = False)


