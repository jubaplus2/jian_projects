## -*- coding: utf-8 -*-
"""
Created on Mon Jun  6 15:02:11 2016

@author: Jubaplus
"""
import datetime
print(datetime.datetime.now())

#####update the direcotry--all output files will be saved in this directory
foldername='/Users/jubaplus1/Desktop/Mama June’s Big Revenge/Sizing'

###### make sure the directory file is up to date
personintab='/Users/jubaplus1/Desktop/Mama June’s Big Revenge/Sizing/personintab140619_161113.csv'
demofilepath='/Users/jubaplus1/Desktop/Mama June’s Big Revenge/Sizing/Demographicpersonlevel.csv'
quadspath='/Users/jubaplus1/Desktop/Mama June’s Big Revenge/Sizing/wetvl3mthufri.csv'
viewershippath='/Users/jubaplus1/Desktop/Mama June’s Big Revenge/Sizing/ViewershipClassificationoneyearnew.csv'
itshowpath='/Users/jubaplus1/Desktop/Mama June’s Big Revenge/mamajune_showaffinitywithclusternew.csv'
otshowpath=''
####put startdate and enddate for the project here:
Startdate=20160813
Enddate=20161113

####it show information: leave it blank if no show falls into the category
###only work for 2 to 4 
numberofitshows=4
itseg_a1=['Totalfreq375479','Totalfreq354977']
itseg_a2=['Totalfreq381963','Totalfreq320037']
itseg_a3=['Totalfreq351491','Totalfreq826306','Totalfreq848177']
itseg_a4=['Totalfreq351491','Totalfreq826306','Totalfreq848177','Totalfreq312073']

###otshownames:
otshownames=[]
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

viewership=pd.read_csv(viewershippath,dtype={'HHID':'object','PersonID':'object'})


df=pd.merge(quads,demo,on=['HHID','PersonID'],how='outer')
df['Quads'].fillna(0,inplace=True)
df=pd.merge(df,intab,on=['HHID','PersonID'],how='outer')
df=pd.merge(df,viewership,on=['HHID','PersonID'],how='outer')


df['HHID_PersonID']=df['HHID']+'_'+df['PersonID']

viewership['HHID_PersonID']=viewership['HHID']+'_'+viewership['PersonID']
ITshows=pd.read_csv(itshowpath)
ITshows=pd.merge(ITshows,viewership,on='HHID_PersonID',how='left')

ITshows['seg_a1']=0
for i in itseg_a1:
    ITshows['seg_a1']=ITshows['seg_a1']+ITshows[i]
    
ITshows['seg_a2']=0
for i in itseg_a2:
    ITshows['seg_a2']=ITshows['seg_a2']+ITshows[i]
    
ITshows['seg_a3']=np.where(((ITshows['Totalfreq351491']>0)&(ITshows['Totalfreq826306']>0)&(ITshows['Totalfreq848177']>0)),1,0)

itseg_a5=itseg_a4

ITshows['seg_a4']=0
for i in itseg_a4:
    for j in itseg_a5:
        if i!=j:
            ITshows['seg_a4']=np.where((ITshows['seg_a4']==1)|((ITshows[i]>0)&(ITshows[j]>0)),1,0)
        
        
ITshows['segment']=np.where(ITshows['seg_a1']>0,"A1",np.where(ITshows['seg_a2']>0,'A2',np.where((ITshows['seg_a3']>0),'A3',np.where(ITshows['seg_a4']>0,'A4','Other'))))

#ITshows=ITshows[ITshows['Totalfreq312073']>0]
print(ITshows.groupby('segment').count())
ITshows=ITshows[['HHID_PersonID', 'segment','ShiftedViewindicatorallprogram']]
ITshows.columns=['HHID_PersonID','Segment','ShiftedViewindicatorallprogram']
ITshows=ITshows.drop_duplicates('HHID_PersonID')

df=pd.merge(ITshows,df,on='HHID_PersonID',how='outer')

'''
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
'''

df.to_csv("AudienceSizingMamajune1219.csv",index=False)


print(datetime.datetime.now())







