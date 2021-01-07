## -*- coding: utf-8 -*-
"""
Created on Mon Jun  6 15:02:11 2016

@author: Jubaplus 
"""
import datetime
print(datetime.datetime.now())

#####update the direcotry--all output files will be saved in this directory
foldername='/Users/jubaplus1/Desktop/WETV/Mary Mary/Sizing'
personintab='/Users/jubaplus1/Desktop/WETV/Mary Mary/Sizing/personintab_160101_170528.csv'
demofilepath='/Users/jubaplus1/Desktop/WETV/Mary Mary/Sizing/Demographicpersonlevel_160101_170528.csv'
quadspath='/Users/jubaplus1/Desktop/WETV/Mary Mary/Sizing/wetv_l6m_0604.csv'
viewershippath=''
itshowpath='/Users/jubaplus1/Desktop/WETV/Mary Mary/mm_showaffnity.csv'
otshowpath='/Users/jubaplus1/Desktop/WETV/Mary Mary/Sizing/marymary_ls.csv'
bcastotindicator='T'
otshowpath2='/Users/jubaplus1/Desktop/WETV/Mary Mary/OT/Cable/programfreq.csv'
otshowpath4=''

####put startdate and enddate for the project here:
Startdate=20160303
Enddate=20170528

gender = ['F']
minage = 25
maxage = 54

#A1 otshowpath AND IN quadspath
#A2 otshowpath AND NOT IN quadspath
#A3 quadspath TOP 30%
#A4 ANY TWO or more shows of itseg_a1 AND Cluster=2/3/5/6/7/8 AND ShiftedViewindicatorallprogram=2or3
#A5 ANY TWO or more shows of itseg_a2 AND Cluster=2/3/5/6/7/8 AND ShiftedViewindicatorallprogram=1
#OT otshownames1 in otshowpath2

####it show information: leave it blank if no show falls into the category
###only work for 2 to 4 
numberofitshows=2
itseg_a1=['Totalfreq320916','Totalfreq841187','Totalfreq406336','Totalfreq363715']
itseg_a2=['Totalfreq363715','Totalfreq406336','Totalfreq408558','Totalfreq402099','Totalfreq320916']
itseg_a3=['']
itseg_a4=['']
###otshownames:
otshownames1=['BRVO _WATCH WHAT HAPPENS LIVE  ','VH1  _LOVE AND HIP HOP 7       ','BET  _MEET THE BROWN           ','BRVO _REAL HOUSEWIVES OF NYC   ','OWN  _HAVES AND THE HAVE NOTS  ','TV1  _FATAL ATTRACTION         ','VH1  _HIP HOP SQUARES          ','BET  _BEING MARY JANE S4       ','BRVO _REAL HOUSEWIVES POTOMAC  ','VH1  _BASKETBALL WIVES 6       ']
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

personintab=intab[['HHID','PersonID','PersonIntab']]
personintab=personintab.groupby(['HHID','PersonID']).mean()
personintab.reset_index(inplace=True)
hhintab=intab[['HHID','HHIntab']]
hhintab=hhintab.drop_duplicates()
hhintab=hhintab.groupby('HHID').mean()
hhintab.reset_index(inplace=True)
intab=pd.merge(personintab,hhintab,on='HHID')

demo=pd.read_csv(demofilepath,dtype={'HHID':'object','PersonID':'object'})
demo = demo[(demo['Age']>=minage)&(demo['Age']<=maxage)&(demo['Gender'].isin(gender))]
quads=pd.read_csv(quadspath,dtype={'HHID':'object','PersonID':'object'})

df=pd.merge(quads,demo,on=['HHID','PersonID'],how='right')
df['Quads'].fillna(0,inplace=True)
df=pd.merge(df,intab,on=['HHID','PersonID'],how='left')

#viewership=pd.read_csv(viewershippath,dtype={'HHID':'object','PersonID':'object'})
#df=pd.merge(df,viewership,on=['HHID','PersonID'],how='outer')

df['HHID_PersonID']=df['HHID']+'_'+df['PersonID']

#viewership['HHID_PersonID']=viewership['HHID']+'_'+viewership['PersonID']
ITshows=pd.read_csv(itshowpath)
#ITshows=pd.merge(ITshows,viewership,on='HHID_PersonID',how='left')



ITshows['A4']=0
for i in itseg_a1:
    ITshows[i]=np.where(ITshows[i]>0,1,0)
    ITshows['A4']=ITshows['A4']+ITshows[i]

ITshows['A5']=0
for i in itseg_a2:
    ITshows[i]=np.where(ITshows[i]>0,1,0)
    ITshows['A5']=ITshows['A5']+ITshows[i]

ITshows['clusterselected'] = np.where(ITshows['Cluster'].isin([2,3,5,6,7,8]),1,0)

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
ITshows=ITshows[['HHID_PersonID', 'A4','A5','ShiftedViewindicatorallprogram','clusterselected']]
ITshows=ITshows.drop_duplicates('HHID_PersonID')

df=pd.merge(ITshows,df,on='HHID_PersonID',how='right')



'''
if bcastotindicator=='T':
    OTshow2=pd.read_csv(otshowpath2)
    OTshows=pd.merge(OTshows,OTshow2,on='HHID_PersonID',how='outer')
    OTshow2=pd.read_csv(otshowpath3)
    OTshows=pd.merge(OTshows,OTshow2,on='HHID_PersonID',how='outer')
'''

OTshows=pd.read_csv(otshowpath,dtype={'HHID':'object','PersonID':'object'})
OTshows['ot']=1
OTshows['HHID_PersonID']=OTshows['HHID']+'_'+OTshows['PersonID']


OTshows=OTshows[['HHID_PersonID','ot']]
OTshows=OTshows.drop_duplicates('HHID_PersonID')

df=pd.merge(OTshows,df,on='HHID_PersonID',how='right')
df.fillna(0,inplace=True)

OTshows2=pd.read_csv(otshowpath2,dtype={'HHID':'object','PersonID':'object'})
OTshows2['ot2']=1
#OTshows2['HHID_PersonID']=OTshows2['HHID']+'_'+OTshows2['PersonID']


OTshows2=OTshows2[['HHID_PersonID','ot2']]
OTshows2=OTshows2.drop_duplicates('HHID_PersonID')

df=pd.merge(OTshows2,df,on='HHID_PersonID',how='right')
df.fillna(0,inplace=True)

df['finalsegment']=np.where((df['Quads']>0)&(df['ot']>0),'A1',
                            np.where((df['Quads']==0)&(df['ot']>0),'A2',
                                     np.where(df['Cluster']=='Core_Occ','A3',
                                              np.where((df['A4']>=2)&(df['clusterselected']==1)&(df['ShiftedViewindicatorallprogram']>=2),'A4',
                                              np.where((df['A5']>=2)&(df['clusterselected']==1)&(df['ShiftedViewindicatorallprogram']==1),'A5',
                                              np.where(df['ot2']==1,'ot','Other'))))))

df.to_csv("AudienceSizing_MaryMary.csv",index=False)
print(datetime.datetime.now())

#test=pd.merge(OTshows2,df,on='HHID_PersonID')
#df['test']=np.where(df['BroadcastOT']>0,1,0)
#df.groupby('test').count()
