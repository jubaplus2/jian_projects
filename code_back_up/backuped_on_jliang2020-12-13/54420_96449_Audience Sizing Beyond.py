# -*- coding: utf-8 -*-
"""
Created on Mon Jun  6 15:02:11 2016

@author: Jubaplus
"""
import datetime
print(datetime.datetime.now())

#####update the direcotry--all output files will be saved in this directory
foldername='/Users/Jubaplus1/Desktop/'

###### make sure the directory file is up to date
directorypath='/Users/jubaplus1/Desktop/Code/New Code/NielsenDirectory.csv'
personintab='/Users/jubaplus1/Desktop/type26_518_820.csv'
demofilepath='/Users/jubaplus1/Desktop/AllDemographicpersonlevel.csv'
quadspath='/Users/jubaplus1/Desktop/Quads.csv'
viewershippath='/Users/jubaplus1/Desktop/ViewershipClassification521_821.csv'
itshowpath='/Users/jubaplus1/Desktop/Freeform/Beyond/showaffinitybeyond 12-34withcluster.csv'
otshowpath='/Users/jubaplus1/Desktop/Freeform/Beyond/OT/Cable/programfreq.csv'
shiftedviewing='/Users/jubaplus1/Desktop/'
####put startdate and enddate for the project here:
Startdate=20160518
Enddate=20160816

####it show information: leave it blank if no show falls into the category
###only work for 2 to 4 
numberofitshows=2
itseg_a1=['Totalfreq397803','Totalfreq385468']
itseg_a2=['Totalfreq842158','Totalfreq397753','Totalfreq347743']
itseg_a3=[]
itseg_a4=[]

###otshownames:
otshownames=['FXX  _SIMPSONS                 ','HALL _GOLDEN GIRLS             ','TNT  _SUPERNATURAL             ','HBOM _BREAK                    ','FX   _HOW I MET YOUR MOTHER    ','DSNY _GIRL MEETS WORLD         ','MTV  _RIDICULOUSNESS SSN7      ','MTV  _CATFISH: THE TV SHOW SSN5','TBSC _KING OF QUEENS           ','TLC  _SAY YES TO THE DRESS     ','TBSC _NEW GIRL                 ','USA  _CHRISLEY KNOWS BEST      ','SPIKE_LIP SYNC BATTLE          ','AMC  _WALKING DEAD             ','TRAV _GHOST ADVENTURES         ','CMDY _AT MIDNIGHT              ','DXD  _ULTIMATE SPIDER-MAN      ','TRAV _FOOD PARADISES           ','DSNY _WALK THE PRANK           ','VH1  _JAMIE FOXX SHOW          ']
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

demo=pd.read_csv(demofilepath,dtype={'HHID':'object','PersonID':'object'})

quads=pd.read_csv(quadspath,dtype={'HHID':'object','PersonID':'object'})

viewership=pd.read_csv(viewershippath,dtype={'HHID':'object','PersonID':'object'})


df=pd.merge(quads,demo,on=['HHID','PersonID'],how='outer')
df['Quads'].fillna(0,inplace=True)
df=pd.merge(df,intab,on=['HHID','PersonID'],how='outer')
df=pd.merge(df,viewership,on=['HHID','PersonID'],how='outer')

df['HHID_PersonID']=df['HHID']+'_'+df['PersonID']

ITshows=pd.read_csv(itshowpath)
ITshows['seg_a1']=0
for i in itseg_a1:
    ITshows['seg_a1']=ITshows['seg_a1']+ITshows[i]
    
ITshows['seg_a2']=0
for i in itseg_a2:
    ITshows['seg_a2']=ITshows['seg_a2']+ITshows[i]
    
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

df.to_csv("AudienceSizing beyondnew.csv",index=False)


print(datetime.datetime.now())








