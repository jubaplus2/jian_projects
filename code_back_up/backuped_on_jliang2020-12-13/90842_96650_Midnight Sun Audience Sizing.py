## -*- coding: utf-8 -*-
"""
Created on Mon Jun  6 15:02:11 2016

@author: Jubaplus
"""
import datetime
print(datetime.datetime.now())

#####update the direcotry--all output files will be saved in this directory
foldername='/Users/jubaplus1/Desktop/Open Road/Midnight Sun/Sizing'

###### make sure the directory file is up to date
personintab='/Users/jubaplus1/Desktop/Open Road/Midnight Sun/Sizing/personintab_150101_170108.csv'
demofilepath='/Users/jubaplus1/Desktop/Open Road/Midnight Sun/Sizing/Demographicpersonlevel_150101_170101.csv'
quadspath='/Users/jubaplus1/Desktop/Open Road/Midnight Sun/Sizing/freefrom_l3m.csv'
viewershippath=''
itshowpath='/Users/jubaplus1/Desktop/Open Road/Midnight Sun/MidnightSun_showaffinity_cluster_code.csv'
otshowpath='/Users/jubaplus1/Desktop/Open Road/Midnight Sun/OT/Cable Show/programfreq.csv'
bcastotindicator='T'
otshowpath2='/Users/jubaplus1/Desktop/Open Road/Midnight Sun/OT/Broadcast Movie/programfreq.csv'
otshowpath3='/Users/jubaplus1/Desktop/Open Road/Midnight Sun/OT/Cable Movie/programfreq.csv'


####put startdate and enddate for the project here:
Startdate=20150221
Enddate=20170108

####it show information: leave it blank if no show falls into the category
###only work for 2 to 4 
numberofitshows=2
itseg_a1=['Totalfreq383013','Totalfreq396471','Totalfreq212525','Totalfreq241277']
itseg_a2=['Totalfreq343641','Totalfreq403644','Totalfreq417458']
itseg_a3=[]
itseg_a4=[]
###otshownames:
otshownames=['NBC  _SAFE HAVEN','FRFM _NOTEBOOK, THE','FRFM _10 THINGS/HATE ABOUT YOU','TBSC _17 AGAIN','FRFM _CHEER SQUAD              ','FRFM _BEYOND                   ','FRFM _PRETTY LITTLE LIARS      ','FRFM _SHADOWHUNTERS            ']


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

df=pd.merge(quads,demo,on=['HHID','PersonID'],how='outer')
df['Quads'].fillna(0,inplace=True)
df=pd.merge(df,intab,on=['HHID','PersonID'],how='outer')

#viewership=pd.read_csv(viewershippath,dtype={'HHID':'object','PersonID':'object'})
#df=pd.merge(df,viewership,on=['HHID','PersonID'],how='outer')

df['HHID_PersonID']=df['HHID']+'_'+df['PersonID']

#viewership['HHID_PersonID']=viewership['HHID']+'_'+viewership['PersonID']
ITshows=pd.read_csv(itshowpath)
#ITshows=pd.merge(ITshows,viewership,on='HHID_PersonID',how='left')

ITshows['seg_a1']=0
for i in itseg_a1:
    ITshows['seg_a1']=ITshows['seg_a1']+ITshows[i]
    
ITshows['seg_a2']=0
for i in itseg_a2:
    ITshows['seg_a2']=ITshows['seg_a2']+ITshows[i]
'''   
itseg_a31=itseg_a3
itseg_a32=itseg_a3
plist=[]
for x in itseg_a3:
    for y in itseg_a31:
        for z in itseg_a32:
            if (x!=y)&(y!=z)&(x!=z):
                a=[x,y,z]
                a.sort()
                var=a[0]+'|'+a[1]+'|'+a[2]
                plist.append(var)
dflist=pd.DataFrame(plist,columns=['plist'])
dflist=dflist.drop_duplicates()

ITshows['seg_a3']=0
for i in dflist['plist']:
    #print(i)
    x=i.split('|')[0]
    y=i.split('|')[1]
    z=i.split('|')[2]
    ITshows['seg_a3']=np.where((ITshows['seg_a3']==1)|((ITshows[x]>0)&(ITshows[y]>0)&(ITshows[z]>0)),1,0)
    

#ITshows['seg_a3']=np.where(((ITshows['Totalfreq351491']>0)&(ITshows['Totalfreq826306']>0)&(ITshows['Totalfreq848177']>0)),1,0)

#itseg_a5=itseg_a4

ITshows['seg_a4']=0
for i in itseg_a4:
    ITshows['seg_a4']=np.where((ITshows['seg_a4']==1)|((ITshows[i]>0)),1,0)
        
'''        
ITshows['segment']=np.where(ITshows['seg_a1']>0,"A1",np.where(ITshows['seg_a2']>0,'A2','Other'))

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
    OTshow2=pd.read_csv(otshowpath3)
    OTshows=pd.merge(OTshows,OTshow2,on='HHID_PersonID',how='outer')

OTshows['OT']=0
for i in otshownames:
    OTshows['OT']=OTshows['OT']+OTshows[i]

OTshows=OTshows[OTshows['OT']>0]
OTshows=OTshows[['HHID_PersonID','OT']]
OTshows=OTshows.drop_duplicates('HHID_PersonID')

df=pd.merge(OTshows,df,on='HHID_PersonID',how='outer')

df.to_csv("AudienceSizingmidnightsun.csv",index=False)
print(datetime.datetime.now())
