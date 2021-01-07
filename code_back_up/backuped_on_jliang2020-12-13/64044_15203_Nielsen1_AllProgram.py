# -*- coding: utf-8 -*-
"""
Created on Fri Sep 16 16:09:12 2016

@author: Jubaplus
"""
import logging
logging.basicConfig(filename='Log_AllProrams.log', level=logging.INFO)
logging.info('File0 PersonIntab')
logging.info('Started')
import datetime
logging.info(datetime.datetime.now())

####put startdate and enddate for the project here:
startdate = 20170409
enddate = 20180401

outputpath = '/home/nielsen/MicroSegment/Engine/Client/AMC/FeartheWalkingDeadv2/'

###code starts here
directorypath='/home/nielsen/Nielsen_Directory_Server/NielsenDirectoryServer.csv'
import datetime
a=datetime.datetime.strptime(str(startdate), '%Y%m%d').date()
b=6-a.weekday()
startdate=a+datetime.timedelta(days=b)
c=datetime.datetime.strptime(str(enddate), '%Y%m%d').date()
d=6-c.weekday()
enddate=c+datetime.timedelta(days=d)

import pandas as pd
directory=pd.read_csv(directorypath)
directory['Week']=pd.to_datetime(directory['Week'])

names30=directory[(directory['Week']<=enddate)&(directory['Week']>=startdate)&((directory['Table']=='C30')|(directory['Table']=='B30'))]
names30=names30['FilePath']

names30RE=directory[(directory['Week']<=enddate)&(directory['Week']>=startdate)&((directory['Table']=='C30RE')|(directory['Table']=='B30RE'))]
names30RE=names30RE['FilePath']

df=pd.DataFrame()
type30=pd.DataFrame()
for i in names30:
    df=pd.read_csv(i,sep='\t')
    df.columns=['all']
    df['type']=df['all'].str[0:2]
    df=df[df['type']=='30']
    type30=type30.append(df,ignore_index=True)
    logging.info(i)
  
type30['NetworkID']=type30['all'].str[8:14]
type30['NetworkName']=type30['all'].str[3:8]
type30['DistributorType']=type30['all'].str[2:3]
type30['ProgramID']=type30['all'].str[14:21]
type30['ProgramName']=type30['all'].str[55:80]
type30['ProgramName']=type30.ProgramName.str.strip()
type30['Programtypesummarycode']=type30['all'].str[122:126]
type30['Programtypesummarycode']=type30.Programtypesummarycode.str.strip()
type30['PremiereIndicator']=type30['all'].str[30:31].astype(int)
type30['ProgramDate']=type30['all'].str[39:47]
type30['ProgramDate']=pd.to_datetime(type30['ProgramDate'])
type30['BroadcastStartTime']=type30['all'].str[86:92]
type30['BroadcastEndTime']=type30['all'].str[92:98]
type30['EpisodeDuration']=type30['all'].str[98:104]
type30['EpisodeName']=type30['all'].str[130:170]
type30['EpisodeName']=type30.EpisodeName.str.strip()
type30['RepeatIndicator']=type30['all'].str[170:171].astype(int)
type30['TelecastN']=type30['all'].str[21:28].astype(int)
type30['SpecialIndicator']=type30['all'].str[28:29].astype(int)
type30['LiveIndicator']=type30['all'].str[224:225].astype(int)
type30['RepossedProgramFlag']=type30['all'].str[292:293]
type30['RepossedProgramAction']=type30['all'].str[293:294]
logging.info(len(type30.index))

####reprocessed program
df=pd.DataFrame()
type30RE=pd.DataFrame()
for i in names30RE:
    df=pd.read_csv(i,sep='\t')
    df.columns=['all']
    df['type']=df['all'].str[0:2]
    df=df[df['type']=='30']
    type30RE=type30RE.append(df,ignore_index=True)
    logging.info(i)
  
type30RE['NetworkID']=type30RE['all'].str[8:14]
type30RE['NetworkName']=type30RE['all'].str[3:8]
type30RE['DistributorType']=type30RE['all'].str[2:3]
type30RE['ProgramID']=type30RE['all'].str[14:21]
type30RE['ProgramName']=type30RE['all'].str[55:80]
type30RE['ProgramName']=type30RE.ProgramName.str.strip()
type30RE['TelecastN']=type30RE['all'].str[21:28].astype(int)
type30RE['RepossedProgramFlag']=type30RE['all'].str[292:293]
type30RE['RepossedProgramAction']=type30RE['all'].str[293:294]
delprogram=type30RE[type30RE['RepossedProgramAction']=='D']
delprogram=delprogram[['NetworkID','ProgramID','TelecastN']]
delprogram['delindicator']=1
type30=pd.merge(type30,delprogram,on=['NetworkID','ProgramID','TelecastN'],how='left')
type30['delindicator'].fillna(0, inplace=True)
type30=type30[type30['delindicator']==0]
type30=type30[['NetworkID','NetworkName','DistributorType','ProgramID','ProgramName','Programtypesummarycode','PremiereIndicator','ProgramDate','BroadcastStartTime','BroadcastEndTime','EpisodeDuration','EpisodeName','RepeatIndicator','TelecastN','SpecialIndicator','LiveIndicator','RepossedProgramFlag','RepossedProgramAction']]

type30RE=type30RE[type30RE['RepossedProgramAction']=='A']
type30RE['Programtypesummarycode']=type30RE['all'].str[122:126]
type30RE['Programtypesummarycode']=type30RE.Programtypesummarycode.str.strip()
type30RE['PremiereIndicator']=type30RE['all'].str[30:31].astype(int)
type30RE['ProgramDate']=type30RE['all'].str[39:47]
type30RE['ProgramDate']=pd.to_datetime(type30RE['ProgramDate'])
type30RE['BroadcastStartTime']=type30RE['all'].str[86:92]
type30RE['BroadcastEndTime']=type30RE['all'].str[92:98]
type30RE['EpisodeDuration']=type30RE['all'].str[98:104]
type30RE['EpisodeName']=type30RE['all'].str[130:170]
type30RE['EpisodeName']=type30RE.EpisodeName.str.strip()
type30RE['RepeatIndicator']=type30RE['all'].str[170:171].astype(int)
type30RE['SpecialIndicator']=type30RE['all'].str[28:29].astype(int)
type30RE['LiveIndicator']=type30RE['all'].str[224:225].astype(int)
type30RE=type30RE[['NetworkID','NetworkName','DistributorType','ProgramID','ProgramName','Programtypesummarycode','PremiereIndicator','ProgramDate','BroadcastStartTime','BroadcastEndTime','EpisodeDuration','EpisodeName','RepeatIndicator','TelecastN','SpecialIndicator','LiveIndicator','RepossedProgramFlag','RepossedProgramAction']]
type30=type30.append(type30RE,ignore_index=True)
type30=type30[['NetworkID','NetworkName','DistributorType','ProgramID','ProgramName','Programtypesummarycode','PremiereIndicator','ProgramDate','BroadcastStartTime','BroadcastEndTime','EpisodeDuration','EpisodeName','RepeatIndicator','TelecastN','SpecialIndicator','LiveIndicator','RepossedProgramFlag','RepossedProgramAction']]
logging.info(len(type30RE.index))
type30=type30.drop_duplicates(['NetworkID','ProgramID','TelecastN'])
logging.info(len(type30.index))

###get seaso
season=type30[['NetworkID','ProgramID','ProgramName']]
season['programnew']=season['ProgramName'].str.lower()
pattern = r'[s][0-9]'
season=season[((season['programnew'].str.contains(pattern))&(season['programnew'].str.contains(' s')))]
season['seasonindex']=season.programnew.str.extract('[s](\d)', expand=False)

season2=type30[['NetworkID','ProgramID','ProgramName']]
season2['programnew']=season2['ProgramName'].str.lower()
season2=season2[((season2['programnew'].str.contains('\d'))&(season2['programnew'].str.contains(' season')))]
season2['seasonindex']=season2.programnew.str.extract('[ ](\d)', expand=False)
season=season.append(season2,ignore_index=True)
season=season.drop_duplicates()
season=season[['NetworkID','ProgramID','seasonindex']]
season=season.drop_duplicates()

###get season from episode name
program=type30[['NetworkID','ProgramID','EpisodeName','Programtypesummarycode','TelecastN']].drop_duplicates()
program['newepname']=program['EpisodeName']
program=program.fillna('')
program=program[program['EpisodeName']!='']
typecode=['A','C','CV','DD','DO','GD','MD','DN','OP','PC','PD','SF','CS','SE','SM']
program=program[program.Programtypesummarycode.isin(typecode)]

replacelist=['A','B','C','D','E','F','G','H','I','J','K','L','M','N',
'O','P','Q','R','S','T','U','V','W','X','Y','Z',
             ' ','-',':','.','\\/',',','&','?',"'",'\\(','\\)','!','$','-','+','#']
for i in replacelist:
    program['newepname']=program['newepname'].str.replace(i,'')
    
program=program[program['newepname']!='']
program['strlen']=program['newepname'].str.len()
program=program[program['strlen']>2]
program['newepname']=program['newepname'].str.replace('0','')
program=program[program['newepname']!='']
program['estseason']=program['newepname'].str[0:1]

program=program[['NetworkID','ProgramID','TelecastN','estseason']]
program=program.drop_duplicates()
program=pd.merge(season,program,on=['NetworkID','ProgramID'],how='outer')
program['seasonindex'].fillna(0, inplace=True)
import numpy as np
program['est_season']=np.where(program['seasonindex']!=0,program['seasonindex'],program['estseason'])
program=program[['NetworkID','ProgramID','TelecastN','est_season']]
program=program.drop_duplicates()
type30=pd.merge(type30,program,on=['NetworkID','ProgramID','TelecastN'],how='left')
type30['est_season'].fillna(0, inplace=True)

program=program[['NetworkID','ProgramID','est_season']]
program=program.drop_duplicates()
program=program.groupby(['NetworkID','ProgramID']).count()
program.reset_index(inplace=True)
program.columns=['NetworkID','ProgramID','estseason_count']
type30=pd.merge(type30,program,on=['NetworkID','ProgramID'],how='left')


###get the premiere date
#premiere=type30[type30['PremiereIndicator']==1]days = {0:'Monday',1:'Tuesday',2:'Wednesday',3:'Thursday',4:'Friday',5:'Saturday',6:'Sunday'}
days = {0:'Monday',1:'Tuesday',2:'Wednesday',3:'Thursday',4:'Friday',5:'Saturday',6:'Sunday'}

type30['BroadcastStartTime24']=type30['BroadcastStartTime'].astype('int')
#type30['BroadcastStartTime24']=np.where(type30['BroadcastStartTime24']>=2400,type30['BroadcastStartTime24']-2400,type30['BroadcastStartTime24'])
premiere=type30[['NetworkID','ProgramID','ProgramDate','BroadcastStartTime24','PremiereIndicator']]
premiere=premiere.drop_duplicates()
premiere1=premiere[premiere['PremiereIndicator']==1]
premiere1=premiere1[['NetworkID','ProgramID','ProgramDate','BroadcastStartTime24']]
premiere1.columns=['NetworkID', 'ProgramID', 'PremiereDate','PremiereTime']
premiere1=premiere1.sort_values(['PremiereDate','PremiereTime'],ascending=[1,1])
programdates=premiere[['NetworkID','ProgramID','ProgramDate','BroadcastStartTime24']]
programdates=programdates.drop_duplicates()
programdates2=premiere[premiere['PremiereIndicator']!=1]
programdates2=programdates2[['NetworkID','ProgramID','ProgramDate','BroadcastStartTime24']]
programdates2=programdates2.drop_duplicates()

premierecounts=premiere[['NetworkID','ProgramID','PremiereIndicator']].groupby(['NetworkID','ProgramID']).sum()
premierecounts.reset_index(inplace=True)
premierecounts0=premierecounts[premierecounts['PremiereIndicator']==0]
premierecounts1=premierecounts[premierecounts['PremiereIndicator']==1]
premierecounts2=premierecounts[premierecounts['PremiereIndicator']>1]

allpremierecounts2=pd.DataFrame()
for i in premierecounts2['ProgramID']:
    df=premiere1[premiere1['ProgramID']==i]
    df=df.reset_index(drop=True)
    df=df.reset_index()
    df2=premiere1[premiere1['ProgramID']==i]
    df2=df2.reset_index(drop=True)
    df2=df2.reset_index()
    df2['index']=df2['index']-1
    df2.columns=['index', 'NetworkID', 'ProgramID', 'Next_PremiereDate','Next_PremiereTime']
    df2['lastindit']=0
    df=pd.merge(df,df2,on=['index', 'NetworkID', 'ProgramID'],how='left')
    df['lastindit'].fillna(1,inplace=True)
    allpremierecounts2=allpremierecounts2.append(df,ignore_index=True)

#allpremierecounts2.to_csv(testpath3) 
premierecounts20=premierecounts2[['NetworkID','ProgramID']]
premierecounts20=pd.merge(premierecounts20,premiere1,on=['NetworkID','ProgramID'])
premierecounts20=premierecounts20.sort_values(['NetworkID','ProgramID','PremiereDate','PremiereTime'],ascending=True)
premierecounts20=premierecounts20.drop_duplicates(['NetworkID','ProgramID'])
premierecounts20=pd.merge(premierecounts20,programdates2,on=['NetworkID','ProgramID'])
premierecounts20=premierecounts20[(premierecounts20['ProgramDate']<premierecounts20['PremiereDate'])|((premierecounts20['ProgramDate']==premierecounts20['PremiereDate'])&(premierecounts20['BroadcastStartTime24']<premierecounts20['PremiereTime']))]
premierecounts20=premierecounts20[['NetworkID','ProgramID','ProgramDate','BroadcastStartTime24']]

premierecounts21=allpremierecounts2[allpremierecounts2['lastindit']==1]
premierecounts21=premierecounts21[['NetworkID','ProgramID','PremiereDate','PremiereTime']]
premierecounts21=pd.merge(premierecounts21,programdates,on=['NetworkID','ProgramID'])
premierecounts21=premierecounts21[(premierecounts21['ProgramDate']>premierecounts21['PremiereDate'])|((premierecounts21['ProgramDate']==premierecounts21['PremiereDate'])&(premierecounts21['BroadcastStartTime24']>=premierecounts21['PremiereTime']))]
premierecounts21=premierecounts21[['NetworkID','ProgramID','ProgramDate','PremiereDate','PremiereTime','BroadcastStartTime24']]

premierecounts22=allpremierecounts2[allpremierecounts2['lastindit']==0]
premierecounts22=premierecounts22[['NetworkID','ProgramID','PremiereDate','Next_PremiereDate','PremiereTime','Next_PremiereTime']]
premierecounts22=pd.merge(premierecounts22,programdates2,on=['NetworkID','ProgramID'])
premierecounts22=premierecounts22[(premierecounts22['ProgramDate']<premierecounts22['Next_PremiereDate'])|((premierecounts22['ProgramDate']==premierecounts22['Next_PremiereDate'])&(premierecounts22['BroadcastStartTime24']<premierecounts22['Next_PremiereTime']))]
premierecounts22=premierecounts22[(premierecounts22['ProgramDate']>premierecounts22['PremiereDate'])|((premierecounts22['ProgramDate']==premierecounts22['PremiereDate'])&(premierecounts22['BroadcastStartTime24']>premierecounts22['PremiereTime']))]
premierecounts22=premierecounts22.sort_values(['NetworkID','ProgramID','PremiereDate','PremiereTime','ProgramDate','BroadcastStartTime24'],ascending=False)
premierecounts22=premierecounts22.drop_duplicates(['NetworkID','ProgramID','PremiereDate','PremiereTime'])
premierecounts22=premierecounts22[['NetworkID', 'ProgramID', 'PremiereDate','PremiereTime','ProgramDate','BroadcastStartTime24']]
premierecounts22.columns=['NetworkID', 'ProgramID', 'PremiereDate','PremiereTime','lastdate','lastbroadcasttime']
premierecounts22['firstdate']=premierecounts22['PremiereDate']
premierecounts22['firstbroadcasttime']=premierecounts22['PremiereTime']
premierecounts22=premierecounts22[['NetworkID','ProgramID','firstdate','firstbroadcasttime','lastdate','lastbroadcasttime','PremiereDate','PremiereTime']]


premierecounts23=premierecounts22[['NetworkID', 'ProgramID', 'PremiereDate','PremiereTime']]
premierecounts23['test']=1
premierecounts23=pd.merge(premierecounts23,allpremierecounts2,on=['NetworkID', 'ProgramID', 'PremiereDate','PremiereTime'],how='outer')
premierecounts23=premierecounts23[premierecounts23['lastindit']==0]
premierecounts23['test'].fillna(0,inplace=True)
premierecounts23=premierecounts23[premierecounts23['test']==0]
premierecounts23=premierecounts23[['NetworkID','ProgramID','PremiereDate','PremiereTime']]
premierecounts23['lastdate']=premierecounts23['PremiereDate']
premierecounts23['lastbroadcasttime']=premierecounts23['PremiereTime']
premierecounts23['firstdate']=premierecounts23['PremiereDate']
premierecounts23['firstbroadcasttime']=premierecounts23['PremiereTime']
premierecounts23=premierecounts23[['NetworkID','ProgramID','firstdate','firstbroadcasttime','lastdate','lastbroadcasttime','PremiereDate','PremiereTime']]
premierecounts22=premierecounts22.append(premierecounts23,ignore_index=True)
premierecounts22=premierecounts22[['NetworkID','ProgramID','firstdate','firstbroadcasttime','lastdate','lastbroadcasttime','PremiereDate','PremiereTime']]
del(premierecounts23)

premierecounts1=premierecounts1[['NetworkID','ProgramID']]
premierecounts1=pd.merge(premierecounts1,programdates,on=['NetworkID','ProgramID'])
premierecounts1=pd.merge(premierecounts1,premiere1,on=['NetworkID','ProgramID'])

premierecounts10=premierecounts1[(premierecounts1['ProgramDate']<premierecounts1['PremiereDate'])|((premierecounts1['ProgramDate']==premierecounts1['PremiereDate'])&(premierecounts1['BroadcastStartTime24']<premierecounts1['PremiereTime']))]
premierecounts10=premierecounts10[['NetworkID','ProgramID','ProgramDate','BroadcastStartTime24']]

premierecounts11=premierecounts1[(premierecounts1['ProgramDate']>premierecounts1['PremiereDate'])|((premierecounts1['ProgramDate']==premierecounts1['PremiereDate'])&(premierecounts1['BroadcastStartTime24']>=premierecounts1['PremiereTime']))]
premierecounts11=premierecounts11[['NetworkID','ProgramID','ProgramDate','BroadcastStartTime24','PremiereDate','PremiereTime']]
premierecounts11=premierecounts11.append(premierecounts21,ignore_index=True)
premierecounts11=premierecounts11.sort_values(['NetworkID','ProgramID','ProgramDate','BroadcastStartTime24'],ascending=False)
premierecounts11=premierecounts11.drop_duplicates(['NetworkID','ProgramID','PremiereDate','PremiereTime'])
premierecounts11=premierecounts11[['NetworkID','ProgramID','ProgramDate','BroadcastStartTime24','PremiereDate','PremiereTime']]
premierecounts11.columns=['NetworkID','ProgramID','lastdate','lastbroadcasttime','PremiereDate','PremiereTime']
premierecounts11['firstdate']=premierecounts11['PremiereDate']
premierecounts11['firstbroadcasttime']=premierecounts11['PremiereTime']
premierecounts11=premierecounts11[['NetworkID','ProgramID','firstdate','firstbroadcasttime','lastdate','lastbroadcasttime','PremiereDate','PremiereTime']]

premierecounts0=premierecounts0[['NetworkID','ProgramID']]
premierecounts0=pd.merge(premierecounts0,programdates,on=['NetworkID','ProgramID'])
premierecounts0=premierecounts0[['NetworkID','ProgramID','ProgramDate','BroadcastStartTime24']]
premierecounts0=premierecounts0.append(premierecounts10,ignore_index=True)
premierecounts0=premierecounts0.append(premierecounts20,ignore_index=True)
premierecounts0min=premierecounts0.sort_values(['NetworkID','ProgramID','ProgramDate','BroadcastStartTime24'],ascending=True)
premierecounts0min=premierecounts0min.drop_duplicates(['NetworkID','ProgramID'])
premierecounts0min.columns=['NetworkID','ProgramID','firstdate','firstbroadcasttime']
premierecounts0max=premierecounts0.sort_values(['NetworkID','ProgramID','ProgramDate','BroadcastStartTime24'],ascending=False)
premierecounts0max=premierecounts0max.drop_duplicates(['NetworkID','ProgramID'])
premierecounts0max.columns=['NetworkID','ProgramID','lastdate','lastbroadcasttime']
premierecounts0=pd.merge(premierecounts0min,premierecounts0max,on=['NetworkID','ProgramID'])
#premierecounts0['PremiereDate']=''
#premierecounts0['PremiereTime']=''
#premierecounts0['PremiereWeekday']=''
premierecounts0=premierecounts0[['NetworkID','ProgramID','firstdate','firstbroadcasttime','lastdate','lastbroadcasttime']]
del(premierecounts0min,premierecounts0max)

premierecounts11['PremiereWeekday']=premierecounts11['PremiereDate'].dt.dayofweek
premierecounts11['PremiereWeekday']= premierecounts11['PremiereWeekday'].apply(lambda x: days[x])
premierecounts22['PremiereWeekday']=premierecounts22['PremiereDate'].dt.dayofweek
premierecounts22['PremiereWeekday']= premierecounts22['PremiereWeekday'].apply(lambda x: days[x])

premierefinal=premierecounts0.append(premierecounts11,ignore_index=True)
premierefinal=premierefinal.append(premierecounts22,ignore_index=True)

testcasts=type30[['NetworkID','ProgramID','TelecastN','ProgramDate','BroadcastStartTime24']]
premierefinal=pd.merge(premierefinal,testcasts,on=['NetworkID','ProgramID'],how='left')
premierefinal=premierefinal[(premierefinal['ProgramDate']<premierefinal['lastdate'])|((premierefinal['ProgramDate']==premierefinal['lastdate'])&(premierefinal['BroadcastStartTime24']<=premierefinal['lastbroadcasttime']))]
premierefinal=premierefinal[(premierefinal['ProgramDate']>premierefinal['firstdate'])|((premierefinal['ProgramDate']==premierefinal['firstdate'])&(premierefinal['BroadcastStartTime24']>=premierefinal['firstbroadcasttime']))]
premierefinal=premierefinal.drop_duplicates(['NetworkID','ProgramID','TelecastN'])
premierefinal=premierefinal[['NetworkID','ProgramID','TelecastN','firstdate','firstbroadcasttime','lastdate','lastbroadcasttime','PremiereDate','PremiereTime','PremiereWeekday']]
#premierefinal.to_csv(testpath2,index=False)

#type30.to_csv(testpath,index=False)

type30=pd.merge(type30,premierefinal,on=['NetworkID','ProgramID','TelecastN'],how='left')
logging.info('mergewithpremiere')
logging.info(len(type30.index))

'''
sesoncheck=type30[['NetworkID','ProgramID','firstdate','lastdate','PremiereDate','PremiereTime','est_season']]
sesoncheck=sesoncheck.drop_duplicates()
sesoncheck=sesoncheck.groupby(['NetworkID','ProgramID','firstdate','lastdate','PremiereDate','PremiereTime']).count()
sesoncheck.columns=['count']
sesoncheck.reset_index(inplace=True)
#sesoncheck.to_csv(testpath4,index=False)

type30=pd.merge(type30,sesoncheck,on=['NetworkID','ProgramID','firstdate','lastdate','PremiereDate','PremiereTime'],how='left')
type30['est_season']=np.where(type30['count']==1,type30['est_season'],0)
'''

#type30['est_season']=type30['est_season'].astype('str')+'_'+type30['firstdate'].astype('str')+'_'+type30['lastdate'].astype('str')
logging.info(len(type30.index))
type30=type30[['NetworkID', 'NetworkName', 'DistributorType',
       'ProgramID', 'ProgramName', 'Programtypesummarycode',
       'PremiereIndicator', 'ProgramDate', 'BroadcastStartTime',
       'BroadcastEndTime', 'EpisodeDuration', 'EpisodeName', 'RepeatIndicator',
       'TelecastN', 'SpecialIndicator', 'LiveIndicator', 'RepossedProgramFlag',
       'RepossedProgramAction', 'est_season', 
       'firstdate','lastdate','PremiereDate', 'PremiereTime',
       'PremiereWeekday']]


type30['TelecastN']=type30['TelecastN'].astype('str') 
type30['TelecastN'] = type30['TelecastN'].apply(lambda x: x.zfill(7))
type30['ProgramID_TelecastN']=type30['ProgramID']+'_'+type30['TelecastN'] 
type30['ProgramFinalName']=type30['NetworkName']+'_'+type30['ProgramName']+'_'+type30['est_season'].astype('str')
type30.to_csv(outputpath + 'allprograms.csv',index=False)

logging.info('Finished')
del(startdate,enddate,type30,df,names30,season,season2)
import gc
gc.collect()
logging.info(datetime.datetime.now())
