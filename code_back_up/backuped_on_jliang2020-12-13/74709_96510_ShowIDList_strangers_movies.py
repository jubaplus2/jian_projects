# -*- coding: utf-8 -*-
"""
Created on Tue Aug 15 13:34:16 2017

@author: Jubaplus
"""

import logging
import datetime
import pandas as pd
import numpy as np

Cable_ID = ['009100','009118','007235','009157']
ProgramID_MovieName = ['STRANGERS THE (2008)','BLAIR WITCH PROJECT, THE','CARRIE (2013)','DONT BREATHE','PURGE, THE: ELECTION YEAR']
#For Program use ProgramID, for movie use movie name
Startdate = 20170201
Enddate = 20180114
outputpath = '/home/nielsen/MicroSegment/Outputs/Quads/'
outputname = 'strangers_movie'

def type30(Cable_ID,MovieName,Startdate,Enddate,broadcast):
    Startdate=datetime.datetime.strptime(str(Startdate), '%Y%m%d').date()
    startdate=Startdate+datetime.timedelta(days=(6-Startdate.weekday()))
    Enddate=datetime.datetime.strptime(str(Enddate), '%Y%m%d').date()
    enddate=Enddate+datetime.timedelta(days=(6-Enddate.weekday()))
    directory=pd.read_csv('/home/nielsen/Nielsen_Directory_Server/NielsenDirectoryServer.csv')
    directory['Week']=pd.to_datetime(directory['Week'])
    if broadcast=='T':
        names30=directory[(directory['Week']<=enddate)&(directory['Week']>=startdate)&(directory['Table'].isin(['C30','B30']))]
    else:
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
        if Cable_ID!=[]:
            df=df[df['CableID'].isin(Cable_ID)]
        type30=type30.append(df,ignore_index=True)
    type30['NetworkName']=type30['all'].str[3:8]
    type30['ProgramID']=type30['all'].str[14:21]
    type30['ProgramName']=type30['all'].str[55:80]
    type30['TelecastN']=type30['all'].str[21:28]
    type30['ProgramDate']=pd.to_datetime(type30['ProgramDate'])
    type30['EpisodeName']=type30['all'].str[130:170]
    type30['EpisodeName']=type30.EpisodeName.str.strip()
    type30 = type30[(type30['ProgramID'].isin(ProgramID_MovieName))|(type30['EpisodeName'].isin(ProgramID_MovieName))]
    return type30

def type36(Cable_ID,Program_ID,Weeklists,broadcast):
    directory=pd.read_csv('/home/nielsen/Nielsen_Directory_Server/NielsenDirectoryServer.csv')
    directory['Week']=pd.to_datetime(directory['Week'])   
    directory = directory[directory['Week'].isin(Weeklists)]
    if broadcast=='T':
        names36=directory[(directory['Table'].isin(['C36','B36']))]
    else:
        names36=directory[(directory['Table']=='C36')]
    names36=names36['FilePath']
    df=pd.DataFrame()
    type36=pd.DataFrame()
    for i in names36:
        df=pd.read_csv(i,sep='\t')
        df.columns=['all']
        df['type']=df['all'].str[0:2]
        df=df[df['type']=='36']
        df['HHID']=df['all'].str[29:36]
        df['CableID']=df['all'].str[3:9]
        if Cable_ID!=[]:
            df=df[df['CableID'].isin(Cable_ID)]
        df['ViewDate']=df['all'].str[68:76]
        df['ViewDate']=pd.to_datetime(df['ViewDate'])
        df['PersonID']=df['all'].str[36:38]
        df=df[df['PersonID']!='00']       
        df['ProgramID']=df['all'].str[9:16]
        df['TelecastN']=df['all'].str[16:23]
        df['ProgramID_TelecastN'] = df['ProgramID'] + '_' + df['TelecastN']
        df=df[df['ProgramID_TelecastN'].isin(Program_ID)]
        df['HHID_PersonID'] = df['HHID'] + '_' + df['PersonID']
        df['ViewStartTime']=df['all'].str[38:44].astype('int')
        df['ViewEndTime']=df['all'].str[44:50].astype('int')  
        df['ShiftedViewCode']=df['all'].str[76:77]
        df['VCRCode']=df['all'].str[55:56]
        df['PlatformCode']=df['all'].str[114:116]
        df['VODIndicator']=df['all'].str[116:118]
        df['DelayMins']=df['all'].str[109:114]
        df['PersonIntab']=df['all'].str[60:66].astype('int')
        df['Age']=df['all'].str[56:59].astype('int')
        df['Gender']=df['all'].str[59:60]
        type36=type36.append(df,ignore_index=True)
        logging.info(i)
    type36=type36[['HHID_PersonID','CableID','ProgramID','TelecastN','ProgramID_TelecastN',
                   'ViewDate','ViewStartTime','ViewEndTime','ShiftedViewCode',
                   'VCRCode','PlatformCode','VODIndicator','DelayMins',
                   'PersonIntab','Age','Gender']]
    return type36
type30table = type30(Cable_ID,ProgramID_MovieName,Startdate,Enddate,'F')
type30table['Week'] = type30table['ProgramDate'].apply(lambda x: x+pd.DateOffset(6-x.weekday()))
type30table['ProgramID_TelecastN'] = type30table['ProgramID'] + '_' + type30table['TelecastN'] 
Program_ID = type30table['ProgramID_TelecastN']
Weeklists = type30table[['Week']].drop_duplicates()
Weeklists = Weeklists['Week']

dfall = type36([],Program_ID,Weeklists,'F')

dfall['mins'] =dfall['ViewEndTime'] - dfall['ViewStartTime'] + 1
dfall.loc[:,'flipfreq'] = 1

intab = dfall[['HHID_PersonID','PersonIntab','ViewDate']].drop_duplicates(['HHID_PersonID','ViewDate'])
intab = intab.groupby(['HHID_PersonID']).mean()
intab.reset_index(inplace = True)

df = dfall[['HHID_PersonID','flipfreq','mins']]
df = df.groupby('HHID_PersonID').sum()
df.reset_index(inplace=True)
df = pd.merge(df,intab[['HHID_PersonID','PersonIntab']],on='HHID_PersonID')
df = df.sort_values('flipfreq',ascending=0)
df = df.reset_index(drop=True)
df = df.reset_index()
df.columns=['freqrank','HHID_PersonID','flipfreq', 'mins','PersonIntab']
df = df.sort_values('mins',ascending=0)
df = df.reset_index(drop=True)
df = df.reset_index()
df.columns=['minsrank','freqrank','HHID_PersonID',  'flipfreq', 'mins','PersonIntab']
rows = len(df.index)
'''
df['Cluster'] = np.where(((df['freqrank']<=rows/10)|(df['minsrank']<=rows/10)),'Core',
                          np.where(((df['freqrank']<=rows/(10/3))|(df['minsrank']<=rows/(10/3))),
                          'Occasional','Balance'))
'''
df['Cluster'] = np.where(df['minsrank']<=rows/10,'Core',
                          np.where(df['minsrank']<=rows/(10/3),
                          'Occasional','Balance'))

df = df[['HHID_PersonID','Cluster','PersonIntab', 'flipfreq', 'mins']]

genderage = dfall[['HHID_PersonID','Age','Gender','ViewDate']]
genderage = genderage.sort_values('ViewDate',ascending = False)
genderage = genderage[['HHID_PersonID','Age','Gender']].drop_duplicates('HHID_PersonID')

df = pd.merge(df,genderage,on='HHID_PersonID')
df.to_csv(outputpath + outputname + '_idlist.csv',index = False)

programs = dfall[['ProgramID_TelecastN']].drop_duplicates()
programs = pd.merge(programs,type30table,on='ProgramID_TelecastN',how='left')
programs.to_csv(outputpath + outputname + '_findshowlist.csv',index = False)



