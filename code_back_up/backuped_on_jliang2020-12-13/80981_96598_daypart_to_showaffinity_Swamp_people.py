# -*- coding: utf-8 -*-
"""
Created on Wed Jun 15 17:52:25 2016

@author: Jubaplus
"""
###

inputpath='/Users/jubaplus1/Desktop/Swamp People/SwampPeopledaypartsnew.csv'
outputpath='/Users/jubaplus1/Desktop/Swamp People/'
#broadcast='/Users/jubaplus1/Desktop/WETV 2017/Output/Broadcast_20161009daypartsnew.csv'
###change below value to 'T' if live +3 is required
##### DO NOT CHANGE THIS !!!!!!!!
live3indicator='F'

###CHANGE DEMO###
minage=25
maxage=54
gender=['F','M']
#minmins=1 mintotalmins=0
mintotalmins=1

### no need to put '0' or ' ' in front of program code
#itlist=[375479,354977,356953,381963,351491,320037,312073,826306,848177,]
#startds=[20160603,20160401,20140619,20160615,20160106,20140718,20140501,20140526,20160104]
#endds=[20160812,20160520,20140814,20160817,20160323,20161113,20160904,20150908,20160222]

###########code starts here, DO NOT Change
import pandas as pd
import numpy as np

df=pd.read_csv(inputpath,sep='\t')
df=df.fillna(0)
df=df[df['PersonIntab']!=0]
df=df.drop_duplicates(['CableNetworkID','ProgramCode','TelecastNumber','HHID_PersonID','ViewershipDate','StartMinuteofProgramViewing','EndMinuteofProgramViewing', 'TimeShiftedViewingCode'])
#if broadcast!='':
#    df2=pd.read_csv(broadcast,sep='\t')
#    df=df.append(df2,ignore_index=True)

#df=df[df['ViewLength']>=minmins]



###RUN THIS CODE FIRST, CHANGE PROGRAM NAME AND CODE###
'''
test=df[['ProgramCode','ProgramName','EpisodeName','BroadcastDate']]
test=test.drop_duplicates()
test.to_csv('/Users/jubaplus1/Desktop/Epnames.csv',index=False)
'''

###PART1 ENDS HERE###


###PART 2 STARTS HERE###
df.dtypes
test=pd.read_csv('/Users/jubaplus1/Desktop/Epnames.csv',dtype={'EpisodeName':'object'})
df=pd.merge(df,test,on=['ProgramCode','ProgramName','EpisodeName','BroadcastDate'])
df['ProgramName']=df['NewProgramName']
df['ProgramCode']=df['NewProgramCode']

'''
df1=pd.DataFrame()
dfall=pd.DataFrame()
for i,j,x in zip(itlist,startds,endds):
    df1=df[df['ProgramCode']==i]
    df1=df1[(df1['BroadcastDate']>=j)&(df1['BroadcastDate']<=x)]
    dfall=dfall.append(df1,ignore_index=True)

df=dfall
'''

df=df[(df['Age']>=minage)&(df['Age']<=maxage)]
df=df[df['SexCode'].isin(gender)]
print(df[['Age','PersonID']].groupby('Age').count())
print(df[['SexCode','PersonID']].groupby('SexCode').count())

Agefile=df[['HHID_PersonID','Age','SexCode']]
Agefile=Agefile.drop_duplicates('HHID_PersonID')

##########,sep="\t")

if live3indicator=='T':
    df=df[df['MinimumPlayDelayMinutes']<=4500]

#########live+3#############
#df=df[df['Minimum Play Delay Minutes']<=4500]

totalfre=df[['CableNetworkID', 'ProgramCode', 'ProgramName','TelecastNumber','HHID_PersonID']]
totalfre=totalfre.drop_duplicates()
totalfre=totalfre.groupby(['HHID_PersonID','ProgramCode', 'ProgramName']).count()
totalfre.reset_index(inplace=True)  
totalfre=totalfre[['HHID_PersonID', 'ProgramCode', 'ProgramName', 'TelecastNumber']]
totalfre.columns=['HHID_PersonID', 'ProgramCode', 'ProgramName', 'Totalfreq']

df['mins']=df['EndMinuteofProgramViewing']-df['StartMinuteofProgramViewing']
totalmins=df[['ProgramCode', 'ProgramName','mins','HHID_PersonID']]
totalmins=totalmins.groupby(['HHID_PersonID','ProgramCode', 'ProgramName']).sum()
totalmins.reset_index(inplace=True)  
totalmins.columns=['HHID_PersonID', 'ProgramCode', 'ProgramName', 'Totalmins']

totalfre2=df[['CableNetworkID', 'ProgramCode', 'ProgramName','TelecastNumber','HHID_PersonID']]
totalfre2=totalfre2.drop_duplicates()
totalfre2=totalfre2[['TelecastNumber','HHID_PersonID']]
totalfre2=totalfre2.groupby(['HHID_PersonID']).count()
totalfre2.reset_index(inplace=True)  
totalfre2.columns=['HHID_PersonID', 'SumofTotalfreq']

totalmins2=df[['mins','HHID_PersonID']]
totalmins2=totalmins2.groupby(['HHID_PersonID']).sum()
totalmins2.reset_index(inplace=True)  
totalmins2.columns=['HHID_PersonID','SumofTotalmins']
totalmins2=totalmins2[totalmins2['SumofTotalmins']>=mintotalmins]



hhintab=df[['PersonIntab','HHID_PersonID']]
hhintab=hhintab.groupby('HHID_PersonID').mean()
hhintab.reset_index(inplace=True)  

shiftview=df[['ProgramCode', 'ProgramName','HHID_PersonID','TimeShiftedViewingCode']]
shiftview=shiftview.drop_duplicates()
shiftview['TimeShiftedViewingCode']=np.where(shiftview['TimeShiftedViewingCode']==1,1,0)
shiftview=shiftview.groupby(['HHID_PersonID','ProgramCode', 'ProgramName']).mean()
shiftview.reset_index(inplace=True)  
shiftview['TimeShiftedViewingCode']=np.where(shiftview['TimeShiftedViewingCode']==1,1,
np.where(shiftview['TimeShiftedViewingCode']==0,2,3))
shiftview.columns=['HHID_PersonID', 'ProgramCode', 'ProgramName', 'ShiftedViewindicator']

shiftview2=df[['HHID_PersonID','TimeShiftedViewingCode']]
shiftview2=shiftview2.drop_duplicates()
shiftview2['TimeShiftedViewingCode']=np.where(shiftview2['TimeShiftedViewingCode']==1,1,0)
shiftview2=shiftview2.groupby(['HHID_PersonID']).mean()
shiftview2.reset_index(inplace=True)  
shiftview2['TimeShiftedViewingCode']=np.where(shiftview2['TimeShiftedViewingCode']==1,1,
np.where(shiftview2['TimeShiftedViewingCode']==0,2,3))
shiftview2.columns=['HHID_PersonID','ShiftedViewindicatorallprogram']
hhintab=pd.merge(hhintab,shiftview2,on='HHID_PersonID',how='left')
hhintab=pd.merge(hhintab,totalfre2,on='HHID_PersonID',how='left')
hhintab=pd.merge(hhintab,totalmins2,on='HHID_PersonID')

allfile=pd.merge(totalfre,totalmins,on=['HHID_PersonID', 'ProgramCode', 'ProgramName'])
allfile=pd.merge(allfile,shiftview,on=['HHID_PersonID', 'ProgramCode', 'ProgramName'])

df2=pd.DataFrame()
program=df[['ProgramCode','ProgramName']]
program=program.drop_duplicates()
for i in program['ProgramCode']:
    df2=allfile[allfile['ProgramCode']==i]
    df2.columns=['HHID_PersonID', 'Program Code'+str(i), 'Program Name'+str(i), 'Totalfreq'+str(i),
       'Totalmins'+str(i), 'ShiftedViewindicator'+str(i)]
    hhintab=pd.merge(hhintab,df2,on='HHID_PersonID',how='left')
hhintab=hhintab.fillna(0)

for i,j in zip(program['ProgramCode'],program['ProgramName']):
    hhintab['Program Code'+str(i)]=i
    hhintab['Program Name'+str(i)]=j

hhintab=pd.merge(hhintab,Agefile,on='HHID_PersonID',how='left')

hhintab.to_csv("/Users/jubaplus1/Desktop/Swamp People/swamp_people_showaffnity.csv",index=False)






