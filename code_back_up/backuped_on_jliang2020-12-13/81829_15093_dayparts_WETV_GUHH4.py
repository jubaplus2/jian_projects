# -*- coding: utf-8 -*-
"""
Created on Mon May 23 10:30:00 2016

@author: Jubaplus
"""
### Change log name here  ###
import logging
logging.basicConfig(filename='log_dayparts_WEtv_GUHH4.log', level=logging.INFO)
logging.info('dayparts')
logging.info('Started')
import datetime
logging.info(datetime.datetime.now())

#####update the direcotry--all output files will be saved in this directory
###change every time. also save code.   no space in file name###
projectname='WEtv_GUHH4'
foldername='/home/nielsen/MicroSegment/Outputs/Dayparts/Wetv/GUHH4/'

###### make sure the directory file is up to date
personintabpath='/home/nielsen/MicroSegment/Outputs/PersonIntab/personintab_160101_180114.csv'
#### Change to the latest date in IT list
#personintabpath2='/disk2/OTProject/MovieOT/personintab161010_161023.csv'
#### put startdate and enddate for the project here:
Startdate = 20170115
Enddate = 20180114

###put IT Shows here, put '000' if you do not want to exclude any IT shows, use , to seprate

itlist=['0397489','0424948','0428228','0423323','0421121','0320916','0433648','0412204','0397262','0418606']

#####shifeted viewing days live+3:4500, live+1:1620, live+7:10080
delay_mins=4500
agelow=25
agehigh=54
gendercode=['F']


#####code starts here, DO NOT make any changes
import os
os.chdir(foldername)
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
import gc

directory=pd.read_csv(directorypath)
directory['Week']=pd.to_datetime(directory['Week'])


names36=directory[(directory['Week']<=enddate)&(directory['Week']>=startdate)&((directory['Table']=='C36')|(directory['Table']=='B36'))]
names30=directory[(directory['Week']<=enddate)&(directory['Week']>=startdate)&((directory['Table']=='C30')|(directory['Table']=='B30'))]
names30=names30['FilePath']
names36=names36['FilePath']

df=pd.DataFrame()
type36=pd.DataFrame()


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
    df=df[df['PersonID']!='00']
    df['HHID_PersonID']=df['HouseholdNumber']+'_'+df['PersonID']
    df['Age']=df['all'].str[56:59].astype('int')
    df=df[(df['Age']>=agelow)&(df['Age']<=agehigh)]
    df['SexCode']=df['all'].str[59:60]
    df=df[df.SexCode.isin(gendercode)]
    df['ppintab36']=df['all'].str[60:66]
    df['StartMinuteofProgramViewing']=df['all'].str[38:44].astype('int')
    df['EndMinuteofProgramViewing']=df['all'].str[44:50].astype('int')
    df['ViewLength']=df['EndMinuteofProgramViewing']-df['StartMinuteofProgramViewing']
    df['VCRRecordOnlyIndicator']=df['all'].str[55:56]
    df['TimeShiftedViewingCode']=df['all'].str[76:77]
    df['ViewingPlatformCode']=df['all'].str[114:116]
    df['MinimumPlayDelayMinutes']=df['all'].str[109:114].astype('int')
    df=df[df['MinimumPlayDelayMinutes']<=delay_mins]
    df['RecentTelecastVODIndicator']=df['all'].str[116:118]
    df=df[['HouseholdNumber','CableNetworkID','ProgramCode','TelecastNumber','PersonID','HHID_PersonID','Age','SexCode','StartMinuteofProgramViewing','EndMinuteofProgramViewing','ViewLength','VCRRecordOnlyIndicator','TimeShiftedViewingCode','MinimumPlayDelayMinutes','ViewingPlatformCode','RecentTelecastVODIndicator','ViewershipDate','ppintab36']]
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
#PersonIntab2=pd.read_csv(personintabpath2,dtype={'HHID':'object','PersonID':'object'})
#PersonIntab=PersonIntab.append(PersonIntab2,ignore_index=True)

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


