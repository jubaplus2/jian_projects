## -*- coding: utf-8 -*-
"""
Created on Mon Jun  6 15:02:11 2016

@author: Jubaplus 
"""
import datetime
print(datetime.datetime.now())

#####update the direcotry--all output files will be saved in this directory
foldername='/Users/jubaplus1/Desktop/Atlanta GUHH/Sizing'
personintab='/Users/jubaplus1/Desktop/Atlanta GUHH/Sizing/personintab_150703_170312.csv'
demofilepath='/Users/jubaplus1/Desktop/Atlanta GUHH/Sizing/Demographicpersonlevel_151101_170305.csv'
quadspath='/Users/jubaplus1/Desktop/Atlanta GUHH/Sizing/wetv_l6m_312.csv'
viewershippath=''
itshowpath='/Users/jubaplus1/Desktop/Atlanta GUHH/GUHHAtl_showaffinity_code.csv'
otshowpath='/Users/jubaplus1/Desktop/Atlanta GUHH/OT/WeTV/WETV_programfreq.csv'
bcastotindicator='T'
otshowpath2='/Users/jubaplus1/Desktop/Atlanta GUHH/OT/Cable/cable_programfreq.csv'
otshowpath4='/Users/jubaplus1/Desktop/Atlanta GUHH/Sizing/marymary_l12m_312.csv'

####put startdate and enddate for the project here:
Startdate=20160312
Enddate=20170312

#A1 shows part in itseg_a1, part in otshownames1, PLUS Mary Mary seperate file. any one of the show
#A2 no shows
#A3 shows itseg_a2,any 2 of these shows
#A4 shows part in otshownames2,part in itseg_a3, any 2 of these shows
#B1-C1 shows in otshownames3, any one of the show

####it show information: leave it blank if no show falls into the category
###only work for 2 to 4 
numberofitshows=3
itseg_a1=['Totalfreq397489','Totalfreq343256']
itseg_a2=['Totalfreq402099','Totalfreq408558','Totalfreq415098','Totalfreq402100']
itseg_a3=['Totalfreq320916','Totalfreq411078']
itseg_a4=['']
###otshownames:
otshownames1=['WETV _CUTTING IT: IN THE ATL   ','WETV _MARRIAGE BOOT CAMP: RS   ']
otshownames2=['VH1  _LOVE & HIP HOP HLLYWD 3  ','VH1  _BLACK INK CREW CHICAGO 2 ']
otshownames3=['VH1  _LOVE AND HIP HOP 7       ','VH1  _LOVE AND HIP HOP 6       ','VH1  _LOVE AND HIP HOP 5       ','VH1  _LOVE AND HIP HOP 4       ','VH1  _LOVE AND HIP HOP 3       ','TV1  _SANFORD & SON            ','BET  _HOUSE OF PAYNE           ','BRVO _DONT BE TARDY            ','BRVO _REAL HSWIVES OF NJ       ','BRVO _MARRIED TO MEDICINE      ','OWN  _QUEEN SUGAR              ','OWN  _HAVES AND THE HAVE NOTS  ','LIF  _LITTLE WOMEN LA          ','OXYG _WAGS MIAMI               ','OWN  _IYANLA, FIX MY LIFE      ','LIF  _RAP GAME                 ','BET  _WENDY WILLIAMS SHOW, THE ','BRVO _FIRST FAMILY OF HIP HOP  ','BRVO _REAL HOUSEWIVES OF NYC   ','VH1  _T.I. AND TINY 5          ']
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

ITshows['A1']=0
for i in itseg_a1:
    ITshows['A1']=ITshows['A1']+ITshows[i]
    
ITshows['A3']=0
for i in itseg_a2:
    ITshows[i]=np.where(ITshows[i]>0,1,0)
    
for i in itseg_a2:
    ITshows['A3']=ITshows['A3']+ITshows[i]

ITshows['A3']=np.where(ITshows['A3']>=2,1,0)

ITshows['A4']=0
for i in itseg_a3:
    ITshows[i]=np.where(ITshows[i]>0,1,0)
    
for i in itseg_a3:
    ITshows['A4']=ITshows['A4']+ITshows[i]




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
ITshows=ITshows[['HHID_PersonID', 'A1', 'A3','A4','ShiftedViewindicatorallprogram']]
ITshows.columns=['HHID_PersonID', 'A1', 'A3','A4','ShiftedViewindicatorallprogram']
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
OTshows['wetvshow']=0
for i in otshownames1:
    OTshows['wetvshow']=OTshows['wetvshow']+OTshows[i]

OTshows2=pd.read_csv(otshowpath2)

OTshows2['vh']=0
for i in otshownames2:
    OTshows2[i]=np.where(OTshows2[i]>0,1,0)
    
for i in otshownames2:
    OTshows2['vh']=OTshows2['vh']+OTshows2[i]

OTshows2['CableOT']=0
for i in otshownames3:
    OTshows2['CableOT']=OTshows2['CableOT']+OTshows2[i]
    
OTshows4=pd.read_csv(otshowpath4,dtype='str')
OTshows4['HHID_PersonID']=OTshows4['HHID']+'_'+OTshows4['PersonID']
OTshows4['mary']=1

OTshows=OTshows[['HHID_PersonID','wetvshow']]
OTshows=OTshows.drop_duplicates('HHID_PersonID')
OTshows2=OTshows2[['HHID_PersonID','vh','CableOT']]
OTshows2=OTshows2.drop_duplicates('HHID_PersonID')
OTshows4=OTshows4[['HHID_PersonID','mary']]
OTshows4=OTshows4.drop_duplicates('HHID_PersonID')

df=pd.merge(OTshows,df,on='HHID_PersonID',how='right')
df=pd.merge(OTshows2,df,on='HHID_PersonID',how='right')
df=pd.merge(OTshows4,df,on='HHID_PersonID',how='right')
df.fillna(0,inplace=True)

df['A42']=df['A4']+df['vh']
df['finalsegment']=np.where((df['A1']>0)|(df['wetvshow']>0)|(df['mary']>0),'A1',
                            np.where(df['A3']>0,'A3',
                                     np.where(df['A42']>=2,'A4',
                                              np.where(df['CableOT']>0,'B','Other'))))

df.to_csv("AudienceSizing_atl_guhh_L12M.csv",index=False)
print(datetime.datetime.now())

#test=pd.merge(OTshows2,df,on='HHID_PersonID')
#df['test']=np.where(df['BroadcastOT']>0,1,0)
#df.groupby('test').count()
