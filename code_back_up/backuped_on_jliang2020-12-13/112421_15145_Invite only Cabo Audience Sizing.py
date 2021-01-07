## -*- coding: utf-8 -*-
"""
Created on Mon Jun  6 15:02:11 2016

@author: Jubaplus 
"""
import datetime
print(datetime.datetime.now())

#####update the direcotry--all output files will be saved in this directory
foldername='/Users/jubaplus1/Desktop/Bravo/Sizing'
personintab='/Users/jubaplus1/Desktop/Bravo/Sizing/personintab_151101_170305.csv'
demofilepath='/Users/jubaplus1/Desktop/Bravo/Sizing/Demographicpersonlevel_151101_170305.csv'
quadspath='/Users/jubaplus1/Desktop/Bravo/Sizing/bravo_l12m.csv'
viewershippath=''
itshowpath='/Users/jubaplus1/Desktop/Bravo/InviteOnly_showaffinity.csv'
otshowpath='/Users/jubaplus1/Desktop/Bravo/OT/Bravo/bravo_programfreq.csv'
bcastotindicator='T'
otshowpath2='/Users/jubaplus1/Desktop/Bravo/OT/Cable/cable_programfreq.csv'
otshowpath4='/Users/jubaplus1/Desktop/Bravo/OT/Broadcast/broadcast_programfreq.csv'

####put startdate and enddate for the project here:
Startdate=20160305
Enddate=20170305

#otshownames1=A1 shows, or statement
#otshownames2=B2 shows, any 2 of the shows
#otshownames3=cable OT shows, or statement
#otshownames4=broadcast OT shows, or statement
#itseg_a1=B1 shows,any 2 of the shows
#itseg_a2=B3 shows, only one show in this group

####it show information: leave it blank if no show falls into the category
###only work for 2 to 4 
numberofitshows=2
itseg_a1=['Totalfreq411078','Totalfreq416086','Totalfreq415600','Totalfreq415098','Totalfreq372070']
itseg_a2=['Totalfreq312073']
itseg_a3=[]
itseg_a4=[]
###otshownames:
otshownames1=['BRVO _REAL HOUSEWIVES ATLANTA  ','BRVO _MARRIED TO MEDICINE      ','BRVO _WATCH WHAT HAPPENS LIVE  ','BRVO _REAL HSWIVES OF NJ       ','BRVO _REAL HOUSEWIVES OF OC    ','BRVO _SHAHS OF SUNSET          ']
otshownames2=['VH1  _LOVE & HIP HOP HLLYWD 3  ','VH1  _LEAVE IT TO STEVIE       ','VH1  _BASKETBALL WIVES LA 5    ','VH1  _BLACK INK CREW CHICAGO 2 ']
otshownames3=['VH1  _LEAVE IT TO STEVIE       ','VH1  _K MICHELLE: MY LIFE 3    ','ENT  _WAGS MIAMI               ','ENT  _WAGS                     ','VH1  _T.I. AND TINY 5          ','ENT  _HOLLYWD MEDIUM           ','VH1  _AMERICAS TOP MODEL 23    ','ENT  _TOTAL BELLAS             ','WETV _MARRIAGE BOOT CAMP: RS   ','ENT  _BOTCHED BY NATURE        ','ENT  _CHRISLEY KNOWS BEST      ','ENT  _REVENGE BODY W KHLOE K   ','VH1  _FRESH PRINCE OF BEL-AIR  ','ENT  _SEX AND THE CITY         ','ENT  _BOTCHED                  ','LIF  _LITTLE WOMEN LA          ','MTV  _TEEN MOM SSN 6           ','ENT  _ROYALS, THE              ','MTV  _CATFISH:THE TV SHOW SSN5B','AEN  _MARRIED AT FIRST ST (A&E)','TLC  _TODDLERS & TIARAS        ','TLC  _FOUR WEDDINGS            ','TLC  _SISTER WIVES             ','TLC  _COUNTING ON              ','AEN  _DOG THE BOUNTY HUNTER    ']
otshownames4=['FOX  _EMPIRE                   ','ABC  _BACHELOR, THE            ','FOX  _STAR                     ']

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

ITshows['B1']=0
for i in itseg_a1:
    ITshows[i]=np.where(ITshows[i]>0,1,0)
    
for i in itseg_a1:
    ITshows['B1']=ITshows['B1']+ITshows[i]

ITshows['B1']=np.where(ITshows['B1']>=2,1,0)

ITshows['B3']=0
for i in itseg_a2:
    ITshows['B3']=ITshows['B3']+ITshows[i]

ITshows['B3']=np.where(ITshows['B3']>0,1,0)


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
#ITshows['segment']=np.where(ITshows['seg_a1']>0,"A1",np.where(ITshows['seg_a2']>0,'A2','Other'))

#ITshows=ITshows[ITshows['Totalfreq312073']>0]
#print(ITshows.groupby('segment').count())
ITshows=ITshows[['HHID_PersonID', 'B1', 'B3','ShiftedViewindicatorallprogram']]
ITshows.columns=['HHID_PersonID', 'B1', 'B3','ShiftedViewindicatorallprogram']
ITshows=ITshows.drop_duplicates('HHID_PersonID')

df=pd.merge(ITshows,df,on='HHID_PersonID',how='outer')



'''
if bcastotindicator=='T':
    OTshow2=pd.read_csv(otshowpath2)
    OTshows=pd.merge(OTshows,OTshow2,on='HHID_PersonID',how='outer')
    OTshow2=pd.read_csv(otshowpath3)
    OTshows=pd.merge(OTshows,OTshow2,on='HHID_PersonID',how='outer')
'''
OTshows=pd.read_csv(otshowpath)
OTshows['A1']=0
for i in otshownames1:
    OTshows['A1']=OTshows['A1']+OTshows[i]

OTshows2=pd.read_csv(otshowpath2)

for i in otshownames2:
    OTshows2[i]=np.where(OTshows2[i]>0,1,0)

OTshows2['B2']=0
for i in otshownames2:
    OTshows2['B2']=OTshows2['B2']+OTshows2[i]
    
OTshows2['B2']=np.where(OTshows2['B2']>=2,1,0)

OTshows2['CableOT']=0
for i in otshownames3:
    OTshows2['CableOT']=OTshows2['CableOT']+OTshows2[i]
    
OTshows4=pd.read_csv(otshowpath4)
OTshows4['BroadcastOT']=0
for i in otshownames4:
    OTshows4['BroadcastOT']=OTshows4['BroadcastOT']+OTshows4[i]

OTshows=OTshows[['HHID_PersonID','A1']]
OTshows=OTshows.drop_duplicates('HHID_PersonID')
OTshows2=OTshows2[['HHID_PersonID','B2','CableOT']]
OTshows2=OTshows2.drop_duplicates('HHID_PersonID')
OTshows4=OTshows4[['HHID_PersonID','BroadcastOT']]
OTshows4=OTshows4.drop_duplicates('HHID_PersonID')


df=pd.merge(OTshows,df,on='HHID_PersonID',how='right')
df=pd.merge(OTshows2,df,on='HHID_PersonID',how='right')
df=pd.merge(OTshows4,df,on='HHID_PersonID',how='right')

df.to_csv("AudienceSizingcabo1L2M.csv",index=False)
print(datetime.datetime.now())

test=pd.merge(OTshows2,df,on='HHID_PersonID')
df['test']=np.where(df['BroadcastOT']>0,1,0)
df.groupby('test').count()
