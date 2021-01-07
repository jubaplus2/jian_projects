## -*- coding: utf-8 -*-
"""
Created on Mon Jun  6 15:02:11 2016

@author: Jubaplus 
"""
import datetime
print(datetime.datetime.now())

#####update the direcotry--all output files will be saved in this directory
foldername='/Users/jubaplus1/Desktop/WETV/WETV-MDM/Sizing'
personintab='/Users/jubaplus1/Desktop/WETV/Tamar and Vince/Sizing/personintab_160101_170319.csv'
demofilepath='/Users/jubaplus1/Desktop/WETV/Tamar and Vince/Sizing/Demographicpersonlevel_160101_170319.csv'
quadspath=''
viewershippath=''
itshowpath='/Users/jubaplus1/Desktop/WETV/WETV-MDM/MDM2_showaffinity_cluster_code.csv'
#MDM LAST SAEASON
otshowpath='/Users/jubaplus1/Desktop/WETV/WETV-MDM/Sizing/MDM_l12m_312.csv'
bcastotindicator='T'
otshowpath2='/Users/jubaplus1/Desktop/WETV/WETV-MDM/OT/WETV/wetv_programfreq.csv'
otshowpath4='/Users/jubaplus1/Desktop/WETV/WETV-MDM/OT/Cable/cable_programfreq.csv'

####put startdate and enddate for the project here:
Startdate=20160312
Enddate=20170312


gender = ['F']
minage = 25
maxage = 54

#A1 file in otshowpath
#A2 no show
#A3 part of file in itshowpath,using itseg_a1. rest in file otshowpath2, using otshownames1. any show
#B1 file in itshowpath, using itseg_a2. ANY TWO SHOWS
#B2 no show
#B3 file in itshowpath, using itseg_a3. ANY TWO SHOWS
#Cable OT file in otshowpath4, using otshownames2

####it show information: leave it blank if no show falls into the category
###only work for 2 to 4 
numberofitshows=3
itseg_a1=['Totalfreq343256','Totalfreq354977']
itseg_a2=['Totalfreq361904','Totalfreq300368','Totalfreq366006']
itseg_a3=['Totalfreq372070','Totalfreq398595','Totalfreq398318']
itseg_a4=['']
###otshownames:
otshownames1=['WETV _MARRIAGE BOOT CAMP: RS   ','WETV _GROWING UP HIP HOP       ','WETV _MY LIFE IS A TELENOVELA  ']
otshownames2=['BRVO _REAL HOUSEWIVES BEV HILLS','BRVO _WATCH WHAT HAPPENS LIVE  ','BRVO _DONT BE TARDY            ','AEN  _DOG THE BOUNTY HUNTER    ','BRVO _REAL HSWIVES OF NJ       ','MTV  _TEEN MOM SSN 6           ','VH1  _BASKETBALL WIVES LA 5    ','LIF  _LITTLE WOMEN LA          ','BRVO _LADIES OF LONDON         ','BRVO _SUMMER HOUSE             ','LIF  _LITTLE WOMEN DALLAS      ','TLC  _90 DAY FIANCE: HAPPILY EV','BRVO _TIMBER CREEK LODGE       ','LIF  _RAP GAME                 ','BRVO _YOURS, MINE OR OURS      ']
otshownames3=['']
otshownames4=['']



import pandas as pd

#OTshows=pd.read_csv(otshowpath)
#otshownames2=pd.DataFrame(OTshows.columns)
#otshownames2.to_csv('/Users/jubaplus1/Desktop/otfiles.csv')

####code start here, do not change
import os
os.chdir(foldername)

import numpy as np
intab=pd.read_csv(personintab,dtype={'HHID':'object','PersonID':'object'})
intab=intab[['HHID','PersonID','PersonIntab']]
intab=intab.groupby(['HHID','PersonID']).mean()
intab.reset_index(inplace=True)

demo=pd.read_csv(demofilepath,dtype={'HHID':'object','PersonID':'object'})
demo = demo[demo['Gender'].isin(gender)]
demo = demo[(demo['Age']>=minage)&(demo['Age']<=maxage)]

#quads=pd.read_csv(quadspath,dtype={'HHID':'object','PersonID':'object'})

df = demo.copy()
#=pd.merge(quads,demo,on=['HHID','PersonID'])
#df['Quads'].fillna(0,inplace=True)
df=pd.merge(df,intab,on=['HHID','PersonID'],how='left')

#viewership=pd.read_csv(viewershippath,dtype={'HHID':'object','PersonID':'object'})

#df=pd.merge(df,viewership,on=['HHID','PersonID'],how='outer')

df['HHID_PersonID']=df['HHID']+'_'+df['PersonID']

ITshows=pd.read_csv(itshowpath)

ITshows['A3']=0
for i in itseg_a1:
    ITshows['A3']=ITshows['A3']+ITshows[i]
    
ITshows['B1']=0
for i in itseg_a2:
    ITshows[i]=np.where(ITshows[i]>0,1,0)
    
for i in itseg_a2:
    ITshows['B1']=ITshows['B1']+ITshows[i]

ITshows['B1']=np.where(ITshows['B1']>=2,1,0)

ITshows['B3']=0
for i in itseg_a3:
    ITshows[i]=np.where(ITshows[i]>0,1,0)
    
for i in itseg_a3:
    ITshows['B3']=ITshows['B3']+ITshows[i]

ITshows['B3']=np.where(ITshows['B3']>=2,1,0)

ITshows=ITshows[['HHID_PersonID', 'A3', 'B1','B3','ShiftedViewindicatorallprogram']]

ITshows=ITshows.drop_duplicates('HHID_PersonID')

df=pd.merge(ITshows,df,on='HHID_PersonID',how='right')


OTshows=pd.read_csv(otshowpath,dtype = 'str')
OTshows['HHID_PersonID']=OTshows['HHID']+'_'+OTshows['PersonID']
OTshows['A1']=1

OTshows2=pd.read_csv(otshowpath2)
OTshows2['A32']=0
for i in otshownames1:
    OTshows2['A32']=OTshows2['A32']+OTshows2[i]

OTshows4=pd.read_csv(otshowpath4)
OTshows4['CableOT']=0
for i in otshownames2:
    OTshows4['CableOT']=OTshows4['CableOT']+OTshows4[i]


OTshows=OTshows[['HHID_PersonID','A1']]
OTshows=OTshows.drop_duplicates('HHID_PersonID')
OTshows2=OTshows2[['HHID_PersonID','A32']]
OTshows2=OTshows2.drop_duplicates('HHID_PersonID')
OTshows4=OTshows4[['HHID_PersonID','CableOT']]
OTshows4=OTshows4.drop_duplicates('HHID_PersonID')

df=pd.merge(OTshows,df,on='HHID_PersonID',how='right')
df=pd.merge(OTshows2,df,on='HHID_PersonID',how='right')
df=pd.merge(OTshows4,df,on='HHID_PersonID',how='right')
df.fillna(0,inplace=True)

df['finalsegment']=np.where((df['A1']>0),'A1',
                            np.where((df['A3']>0)|((df['A32']>0)),'A3',
                            np.where(df['B1']>0,'B1',
                            np.where(df['B3']>0,'B3',
                            np.where(df['CableOT']>0,'CableOT','Other')))))

df.to_csv("AudienceSizing_MDM.csv",index=False)
print(df[['finalsegment','PersonIntab']].groupby('finalsegment').sum())
print(datetime.datetime.now())
