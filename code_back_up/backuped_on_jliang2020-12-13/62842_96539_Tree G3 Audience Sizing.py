## -*- coding: utf-8 -*-
"""
Created on Mon Jun  6 15:02:11 2016

@author: Jubaplus 
"""
import datetime
print(datetime.datetime.now())

#####update the direcotry--all output files will be saved in this directory
foldername='/Users/jubaplus1/Desktop/TV1/Fatal_Attraction 5:15/Sizing'
personintab='/Users/jubaplus1/Desktop/TV1/Fatal_Attraction 5:15/Sizing/personintab_160101_170416.csv'
demofilepath='/Users/jubaplus1/Desktop/TV1/Fatal_Attraction 5:15/Sizing/Demographicpersonlevel_160101_170416.csv'
#fatal attraction last two seasons
quadspath='/Users/jubaplus1/Desktop/TV1/Fatal_Attraction 5:15/Sizing/fa_l2s.csv'
viewershippath=''
itshowpath='/Users/jubaplus1/Desktop/TV1/Fatal_Attraction 5:15/Sizing/FA_showaffinity_cluster.csv'
#TV1 Last 6 month
otshowpath='/Users/jubaplus1/Desktop/TV1/Fatal_Attraction 5:15/Sizing/tv1_l6m_416.csv'
bcastotindicator='T'
otshowpath2='/Users/jubaplus1/Desktop/TV1/Fatal_Attraction 5:15/OT/Cable/cable_programfreq.csv'
otshowpath4=''

####put startdate and enddate for the project here:
Startdate=20160101
Enddate=20170416

gender = ['F','M']
minage = 25
maxage = 54

#A1 quadspath and itseg_a1, any show
#A2 otshowpath and (AA women age 35-54)
#A3 itseg_a2, any show
#A4 itseg_a3, any show
#OT otshowpath2 in otshownames1

####it show information: leave it blank if no show falls into the category
###only work for 2 to 4 
numberofitshows=3
itseg_a1=['Totalfreq395960']
itseg_a2=['Totalfreq365623','Totalfreq354396','Totalfreq314387']
itseg_a3=['Totalfreq287143','Totalfreq287143']
itseg_a4=['']
###otshownames:
otshownames1=['AEN  _THE FIRST 48             ','ID   _HOMICIDE HUNTER LT JOE KE','OWN  _DR. PHIL                 ','BET  _MEET THE BROWN           ','VH1  _JAMIE FOXX SHOW          ','ID   _WEB OF LIES              ','BET  _HOUSE OF PAYNE           ','VH1  _LOVE & HIP HOP HLLYWD 3  ','BRVO _MARRIED TO MEDICINE      ','ID   _DISAPPEARED              ','ID   _ON THE CASE WITH PZ      ','WGNA _IN THE HEAT OF THE NIGHT ','ID   _SWAMP MURDERS            ','ID   _MURDER COMES TO TOWN     ','ID   _NIGHTMARE NEXT DOOR      ']
otshownames2=['']
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

quads=pd.read_csv(quadspath,dtype={'HHID':'object','PersonID':'object'})

df = pd.merge(quads,demo,on=['HHID','PersonID'],how='right')
df['Quads'].fillna(0,inplace=True)

df=pd.merge(df,intab,on=['HHID','PersonID'],how='left')

#viewership=pd.read_csv(viewershippath,dtype={'HHID':'object','PersonID':'object'})

#df=pd.merge(df,viewership,on=['HHID','PersonID'],how='outer')

df['HHID_PersonID']=df['HHID']+'_'+df['PersonID']

ITshows=pd.read_csv(itshowpath)

ITshows['A1']=0
for i in itseg_a1:
    ITshows['A1']=ITshows['A1']+ITshows[i]
    
ITshows['A3']=0
#'ShiftedViewindicatorallprogram'
for i in itseg_a2:
    ITshows['A3']=ITshows['A3']+ITshows[i]

ITshows['A4']=0
#'ShiftedViewindicatorallprogram'
for i in itseg_a3:
    ITshows['A4']=ITshows['A4']+ITshows[i]
    
ITshows['A5']=0
#'ShiftedViewindicatorallprogram'
for i in itseg_a3+itseg_a2:
    ITshows[i]=np.where(ITshows[i]>0,1,0)
    ITshows['A5']=ITshows['A5']+ITshows[i]


ITshows=ITshows[['HHID_PersonID','A1', 'A3', 'A4', 'A5','ShiftedViewindicatorallprogram']]

ITshows=ITshows.drop_duplicates('HHID_PersonID')

df=pd.merge(ITshows,df,on='HHID_PersonID',how='right')


OTshows=pd.read_csv(otshowpath,dtype = 'str')
OTshows['HHID_PersonID']=OTshows['HHID']+'_'+OTshows['PersonID']
OTshows['A1OT']=1

OTshows2=pd.read_csv(otshowpath2)
OTshows2['OT']=0
for i in otshownames1:
    OTshows2['OT']=OTshows2['OT']+OTshows2[i]


OTshows=OTshows[['HHID_PersonID','A1OT']]
OTshows=OTshows.drop_duplicates('HHID_PersonID')
OTshows2=OTshows2[['HHID_PersonID','OT']]
OTshows2=OTshows2.drop_duplicates('HHID_PersonID')



df=pd.merge(OTshows,df,on='HHID_PersonID',how='right')
df=pd.merge(OTshows2,df,on='HHID_PersonID',how='right')
df.fillna(0,inplace=True)

df['finalsegment']=np.where((df['Quads']>0)&(df['A1']>0),'A1',
                            np.where((df['A1OT']>0)&((df['Race']==1)&(df['Age']>=35)&(df['Age']<=54)&(df['Gender']=='F')),'A2',
                            np.where(df['A5']>=2,'A3',
                            np.where(df['OT']>0,'OT','Other'))))

df.to_csv("AudienceSizing_fa_G3.csv",index=False)
print(df[['finalsegment','PersonIntab']].groupby('finalsegment').sum())
print(datetime.datetime.now())
