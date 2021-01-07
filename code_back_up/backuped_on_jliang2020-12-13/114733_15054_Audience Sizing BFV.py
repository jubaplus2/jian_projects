## -*- coding: utf-8 -*-
"""
Created on Mon Jun  6 15:02:11 2016

@author: Jubaplus
"""
import datetime
import numpy as np
print(datetime.datetime.now())

#####update the direcotry--all output files will be saved in this directory
foldername='/Users/jubaplus1/Desktop/BFV S5B/Sizing'

###### make sure the directory file is up to date
personintab='/Users/jubaplus1/Desktop/Open Road/The Promise/Sizing/personintab_150101_170108.csv'
demofilepath='/Users/jubaplus1/Desktop/Open Road/The Promise/Sizing/Demographicpersonlevel_150101_170101.csv'
quadspath='/Users/jubaplus1/Desktop/BFV S5B/Sizing/WE_l3m.csv'
viewershippath=''
itshowpath='/Users/jubaplus1/Desktop/BFV S5B/BFVS5B cluster new.csv'
otshowpath='/Users/jubaplus1/Desktop/BFV S5B/OT/Cable/programfreq.csv'
####put startdate and enddate for the project here:
Startdate=20150101
Enddate=20170108

minage=25
maxage=54
gender=['F']
####it show information: leave it blank if no show falls into the category
###only work for 2 to 4 
numberofitshows=5
itseg_a1=['Totalfreq343256']
itseg_a2=['Totalfreq410512','Totalfreq358203','Totalfreq397489']
itseg_a3=['Totalfreq410052','Totalfreq402099','Totalfreq320916']
itseg_a5=['Totalfreq410052','Totalfreq402099','Totalfreq320916']
itseg_a4=['Totalfreq410052','Totalfreq402099']

###otshownames:
ot2path='/Users/jubaplus1/Downloads/programfreq.csv'
otshow2=['WETV _CUTTING IT: IN THE ATL   ','WETV _MATCH MADE IN HEAVEN     ']
otshownames=['TV1  _EMPIRE                   ','VH1  _BASKETBALL WIVES LA 5    ','VH1  _BASKETBALL WIVES LA 5 TRL','VH1  _BASKETBALL WIVES LA 4    ','VH1  _BLACK INK CREW 4         ','VH1  _BLACK INK CREW CHICAGO   ','VH1  _BLACK INK CREW CHICAGO 2 ']
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

#import numpy as np
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
demo=demo[(demo['Age']>=minage)&(demo['Age']<=maxage)&(demo['Gender'].isin(gender))]

quads=pd.read_csv(quadspath,dtype={'HHID':'object','PersonID':'object'})

#viewership=pd.read_csv(viewershippath,dtype={'HHID':'object','PersonID':'object'})


df=pd.merge(quads,demo,on=['HHID','PersonID'],how='right')
df['Quads'].fillna(0,inplace=True)

df=pd.merge(df,intab,on=['HHID','PersonID'])#,how='outer')
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

#ITshows['seg_a4']=0

ITshows['seg_a4']=np.where((ITshows[itseg_a4[0]]>0)&(ITshows[itseg_a4[1]]>0),1,0)
 
ITshows['seg_a5']=0
for i in itseg_a5:
    ITshows['seg_a5']=ITshows['seg_a5']+ITshows[i]
 
  
OTshowsforA=pd.read_csv(ot2path) 
OTshowsforA['OT2']=0
for i in otshow2:
    OTshowsforA['OT2']=OTshowsforA['OT2']+OTshowsforA[i]

OTshowsforA=OTshowsforA[OTshowsforA['OT2']>0]
OTshowsforA=OTshowsforA[['HHID_PersonID','OT2']]
OTshowsforA=OTshowsforA.drop_duplicates('HHID_PersonID')

ITshows=pd.merge(OTshowsforA,ITshows,on='HHID_PersonID',how='right')   
ITshows['OT2'].fillna(0,inplace=True)       
ITshows['segment']=np.where(ITshows['seg_a1']>0,"A1",np.where((ITshows['seg_a2']>0)|(ITshows['OT2']>0),'A2',np.where((ITshows['seg_a3']>0),'A3',np.where(ITshows['seg_a5']>0,'A5',np.where(ITshows['seg_a4']>0,'A4','Other')))))

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

df.to_csv("AudienceSizingBFVNEW.csv",index=False)


print(datetime.datetime.now())







