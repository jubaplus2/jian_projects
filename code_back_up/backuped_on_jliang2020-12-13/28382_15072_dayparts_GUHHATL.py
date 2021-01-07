# -*- coding: utf-8 -*-
"""
Created on Mon May 23 10:30:00 2016

@author: Jubaplus
"""
import logging
logging.basicConfig(filename='dayparts_GUHHATL_20161025.log', level=logging.INFO)
logging.info('dayparts')
logging.info('Started')
import datetime
logging.info(datetime.datetime.now())

#####update the direcotry--all output files will be saved in this directory
###change every time. also save code.no space in file name###
projectname='GUHHATL_20161009'
foldername='/disk2/Projects/WETV_Planning_2017/GUHHATL/'

###### make sure the directory file is up to date
personintabpath='/disk2/OTProject/MovieOT/personintab150217_161009.csv'
####put startdate and enddate for the project here:
Startdate = 20150505	
Enddate = 20161009

###put IT Shows here, put '000' if you do not want to exclude any IT shows, use to seprate

itlist=['0397489','0841187','0402099','0397262','0364648','0402100','0380593','0363715','0385186']


#####code starts here, DO NOT make any changes
import os
os.chdir(foldername)
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
import gc

directory=pd.read_csv(directorypath)
directory['Week']=pd.to_datetime(directory['Week'])


names36=directory[(directory['Week']<=enddate)&(directory['Week']>=startdate)&((directory['Table']=='C36')|(directory['Table']=='B36'))]
names30=directory[(directory['Week']<=enddate)&(directory['Week']>=startdate)&((directory['Table']=='C30')|(directory['Table']=='B30'))]
names30=names30['FilePath']
names36=names36['FilePath']

df=pd.DataFrame()
type36=pd.DataFrame()
df2=pd.DataFrame()
hhintab36=pd.DataFrame()

for i in names36:
    df=pd.read_csv(i,sep='\t')
    df.columns=['all']
    df['type']=df['all'].str[0:2]
    df=df[df['type']=='36']
    df['CableNetworkID']=df['all'].str[3:9]
    df['ProgramCode']=df['all'].str[9:16]
    df=df[df.ProgramCode.isin(itlist)]
    df['HouseholdNumber']=df['all'].str[29:36]
    df['ViewershipDate']=df['all'].str[68:76]
    df['PersonID']=df['all'].str[36:38]
    df['TelecastNumber']=df['all'].str[16:23]
    df2=df[df['PersonID']=='00']
    df2['hhintab36']=df2['all'].str[60:66]
    df2=df2[df2['hhintab36']!='000000']
    df2=df2[['CableNetworkID','ProgramCode','TelecastNumber','HouseholdNumber','ViewershipDate','hhintab36']]
    df2=df2.drop_duplicates(['CableNetworkID','ProgramCode','TelecastNumber','HouseholdNumber','ViewershipDate'])
    logging.info(len(df2.index))
    df=df[df['PersonID']!='00']
    df['HHID_PersonID']=df['HouseholdNumber']+'_'+df['PersonID']
    df['Age']=df['all'].str[56:59]
    df['SexCode']=df['all'].str[59:60]
    df['ppintab36']=df['all'].str[60:66]
    df['StartMinuteofProgramViewing']=df['all'].str[38:44].astype('int')
    df['EndMinuteofProgramViewing']=df['all'].str[44:50].astype('int')
    df['ViewLength']=df['EndMinuteofProgramViewing']-df['StartMinuteofProgramViewing']
    df['VCRRecordOnlyIndicator']=df['all'].str[55:56]
    df['TimeShiftedViewingCode']=df['all'].str[76:77]
    df['ViewingPlatformCode']=df['all'].str[114:116]
    df['MinimumPlayDelayMinutes']=df['all'].str[109:114]
    df['RecentTelecastVODIndicator']=df['all'].str[116:118]
    df=df[['HouseholdNumber','CableNetworkID','ProgramCode','TelecastNumber','PersonID','HHID_PersonID','Age','SexCode','StartMinuteofProgramViewing','EndMinuteofProgramViewing','ViewLength','VCRRecordOnlyIndicator','TimeShiftedViewingCode','MinimumPlayDelayMinutes','ViewingPlatformCode','RecentTelecastVODIndicator','ViewershipDate','ppintab36']]
    logging.info(len(df.index))
    df=pd.merge(df,df2,on=['HouseholdNumber','CableNetworkID','ProgramCode','TelecastNumber','ViewershipDate'])
    logging.info(len(df.index))
    type36=type36.append(df,ignore_index=True)
    logging.info(i)



logging.info(len(type36.index))
type36=type36.drop_duplicates()
logging.info(len(type36.index))

df=pd.DataFrame()
type30=pd.DataFrame()
for i in names30:
    df=pd.read_csv(i,sep='\t')
    df.columns=['all']
    df['type']=df['all'].str[0:2]
    df=df[df['type']=='30']
    df['BroadcastDate']=df['all'].str[39:47]
    df['CableNetworkID']=df['all'].str[8:14]
    type30=type30.append(df,ignore_index=True)
  

type30['ProgramDistributor']=type30['all'].str[3:8]
type30['ProgramCode']=type30['all'].str[14:21]
type30['ProgramName']=type30['all'].str[55:80]
type30['TelecastNumber']=type30['all'].str[21:28]
type30['ReportedStartTime_Program']=type30['all'].str[88:90]
type30['EpisodeName']=type30['all'].str[130:170]
type30['EpisodeName']=type30.EpisodeName.str.strip()
type30['ReportedDurationProgramDuration']=type30['all'].str[98:104]
logging.info(len(type30.index))
type30=type30[['BroadcastDate','CableNetworkID','ProgramDistributor','ProgramCode','ProgramName','TelecastNumber','ReportedStartTime_Program','EpisodeName','ReportedDurationProgramDuration']]

allfile=pd.merge(type30,type36,on=['CableNetworkID','ProgramCode','TelecastNumber'])
logging.info(len(allfile.index))
allfile=allfile.drop_duplicates()
logging.info(len(allfile.index))


allfile['ViewershipDate']=pd.to_datetime(allfile['ViewershipDate'])
filedircname=projectname+'daypartsnewintabfrom36.csv'
allfile.to_csv(filedircname,index=False,sep='\t')

PersonIntab=pd.read_csv(personintabpath,dtype={'HHID':'object','PersonID':'object'})
PersonIntab['HHID_PersonID']=PersonIntab['HHID']+'_'+PersonIntab['PersonID']
PersonIntab=PersonIntab[['HHID_PersonID','ViewDate','PersonIntab','HHIntab']]
PersonIntab['ViewDate']=pd.to_datetime(PersonIntab['ViewDate'])
PersonIntab.columns=['HHID_PersonID','ViewershipDate','PersonIntab','HHIntab']
allfile=pd.merge(allfile,PersonIntab,on=['HHID_PersonID','ViewershipDate'])

filedircname=projectname+'daypartsnew.csv'
allfile.to_csv(filedircname,index=False,sep='\t')

del df
del type30
del type36
gc.collect()

logging.info(datetime.datetime.now())
logging.info('Finished')


