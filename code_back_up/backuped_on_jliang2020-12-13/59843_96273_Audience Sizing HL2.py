## -*- coding: utf-8 -*-
"""
Created on Mon Jun  6 15:02:11 2016

@author: Jubaplus
"""
import datetime
print(datetime.datetime.now())

#####update the direcotry--all output files will be saved in this directory
foldername='/Users/jubaplus1/Desktop/Hap & Leonard S2/Sizing'

###### make sure the directory file is up to date
directorypath='/Users/jubaplus1/Desktop/Code/New Code/NielsenDirectory.csv'
personintab='/Users/jubaplus1/Desktop/Hap & Leonard S2/Sizing/type26_16325_16925.csv'
demofilepath='/Users/jubaplus1/Desktop/Hap & Leonard S2/Sizing/AllDemographicpersonleveltest.csv'
quadspath='/Users/jubaplus1/Desktop/Hap & Leonard S2/Sizing/QuadsSundanceL3M.csv'
viewershippath=''
itshowpath='/Users/jubaplus1/Desktop/Hap & Leonard S2/H&L2_personlevel_25_54_L3 with cluster.csv'
otshowpath='/Users/jubaplus1/Desktop/Hap & Leonard S2/OT/Cable/programfreq.csv'
####put startdate and enddate for the project here:
Startdate=20160625
Enddate=20160925

####it show information: leave it blank if no show falls into the category
###only work for 2 to 4 
numberofitshows=4
itseg_a1=['Totalfreq400173']
itseg_a2=['Totalfreq382631','Totalfreq285477','Totalfreq363137']
itseg_a3=['Totalfreq382631','Totalfreq285477','Totalfreq363137']
itseg_a4=['Totalfreq406336']

###otshownames:
otshownames=['HLN  _FORENSIC FILES           ','SPIKE_COPS                     ','HIST _AMERICAN PICKERS         ','FOOD _DINERS, DRIVE INS & DIVES','TNT  _LAW & ORDER              ','AEN  _STORAGE WARS             ','TBSC _SEINFELD                 ','ESPN _SPORTSCENTER AM      L   ','HGTV _FLIP OR FLOP             ','USA  _MODERN FAMILY            ','TV1  _SANFORD & SON            ','AEN  _DUCK DYNASTY             ','OWN  _DR. PHIL                 ','AEN  _DOG THE BOUNTY HUNTER    ','BET  _MARTIN                   ','OWN  _DATELINE ON OWN          ','AEN  _CRIMINAL MINDS           ','AEN  _60 DAYS IN               ','ID   _HOMICIDE HUNTER LT JOE KE','OWN  _HAVES AND THE HAVE NOTS  ']
####change value to T if BroadcastOT exists
bcastotindicator='F'
otshowpath2='/Users/jubaplus1/Desktop/WETV MBC S6/OT/programfreq.csv'
import pandas as pd

#OTshows=pd.read_csv(otshowpath)
#otshownames2=pd.DataFrame(OTshows.columns)
#otshownames2.to_csv('/Users/jubaplus1/Desktop/otfiles.csv')

####code start here, do not change
import os
os.chdir(foldername)

import numpy as np
intab=pd.read_csv(personintab,dtype={'HHID':'object','PersonID':'object'})

personintab=intab[['HHID','PersonID','PersonIntab']]
personintab=personintab.groupby(['HHID','PersonID']).mean()
personintab.reset_index(inplace=True)
hhintab=intab[['HHID','HHIntab']]
hhintab=hhintab.drop_duplicates()
hhintab=hhintab.groupby('HHID').mean()
hhintab.reset_index(inplace=True)
intab=pd.merge(personintab,hhintab,on='HHID')

demo=pd.read_csv(demofilepath,dtype={'HHID':'object','PersonID':'object'})

quads=pd.read_csv(quadspath,dtype={'HHID':'object','PersonID':'object'})

#viewership=pd.read_csv(viewershippath,dtype={'HHID':'object','PersonID':'object'})


df=pd.merge(quads,demo,on=['HHID','PersonID'],how='outer')
df['Quads'].fillna(0,inplace=True)
df=pd.merge(df,intab,on=['HHID','PersonID'],how='outer')
#df=pd.merge(df,viewership,on=['HHID','PersonID'],how='outer')

df['HHID_PersonID']=df['HHID']+'_'+df['PersonID']

ITshows=pd.read_csv(itshowpath)
ITshows['seg_a1']=0
for i in itseg_a1:
    ITshows['seg_a1']=ITshows['seg_a1']+ITshows[i]
    
ITshows['seg_a2']=np.where(((ITshows['Totalfreq382631']>0)&(ITshows['Totalfreq285477']>0)&(ITshows['Totalfreq363137']>0)),1,0)
    
if numberofitshows==2:
    ITshows['segment']=np.where(ITshows['seg_a1']>0,"A1",np.where(ITshows['seg_a2']>0,'A2','Other'))

if numberofitshows==3:
    ITshows['seg_a3']=0
    for i in itseg_a3:
        ITshows['seg_a3']=ITshows['seg_a3']+ITshows[i]
    ITshows['segment']=np.where(ITshows['seg_a1']>0,"A1",np.where(ITshows['seg_a2']>0,'A2',np.where(ITshows['seg_a3']>0,'A3','Other')))

if numberofitshows==4:
    ITshows['seg_a3']=0
    for i in itseg_a3:
        ITshows['seg_a3']=ITshows['seg_a3']+ITshows[i]
    ITshows['seg_a4']=0
    for i in itseg_a4:
        ITshows['seg_a4']=ITshows['seg_a4']+ITshows[i]
    ITshows['segment']=np.where(ITshows['seg_a1']>0,"A1",np.where(ITshows['seg_a2']>0,'A2',np.where(ITshows['seg_a3']>0,'A3',np.where(ITshows['seg_a4']>0,'A4','Other'))))

#ITshows=ITshows[ITshows['Totalfreq312073']>0]
print(ITshows.groupby('segment').count())
ITshows=ITshows[['HHID_PersonID', 'segment','ShiftedViewindicatorallprogram']]
ITshows.columns=['HHID_PersonID','Segment','ShiftedViewindicatorallprogram']
ITshows=ITshows.drop_duplicates('HHID_PersonID')

df=pd.merge(ITshows,df,on='HHID_PersonID',how='outer')

OTshows=pd.read_csv(otshowpath)

if bcastotindicator=='T':
    OTshow2=pd.read_csv(otshowpath2)
    OTshows=pd.merge(OTshows,OTshow2,on='HHID_PersonID',how='outer')

OTshows['OT']=0
for i in otshownames:
    OTshows['OT']=OTshows['OT']+OTshows[i]

OTshows=OTshows[OTshows['OT']>0]
OTshows=OTshows[['HHID_PersonID','OT']]

OTshows=OTshows[['HHID_PersonID','OT']]
OTshows=OTshows.drop_duplicates('HHID_PersonID')

df=pd.merge(OTshows,df,on='HHID_PersonID',how='outer')

df.to_csv("AudienceSizing H&LS2 10.21 v2.csv",index=False)


print(datetime.datetime.now())








