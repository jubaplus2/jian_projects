## -*- coding: utf-8 -*-
"""
Created on Mon Jun  6 15:02:11 2016

@author: Jubaplus 
"""
import datetime
print(datetime.datetime.now())


#####update the direcotry--all output files will be saved in this directory
foldername='/Users/jubaplus1/Desktop/Z living/Sizing'
personintab='/Users/jubaplus1/Desktop/WGNA/Bellevue-Jan/Sizing/personintab_160101_170903.csv'
demofilepath='/Users/jubaplus1/Desktop/Z living/Sizing/Demographicpersonlevel_160101_170910.csv'
quadspath=''
viewershippath=''
itshowpath='/Users/jubaplus1/Desktop/Z living/HousePoor/housepoor_showaffinity_cluster_code.csv'
otshowpath='/Users/jubaplus1/Desktop/Z living/HousePoor/OT/programfreq.csv'
bcastotindicator='F'
otshowpath1 = ['']
ga1='/Users/jubaplus1/Desktop/Z living/Sizing/zliving_housepoor_l12m.csv'

####put startdate and enddate for the project here:
Startdate=20160903
Enddate=20170903
 
gender = ['F']
minage = 25
maxage = 54

# for all groups, mvpd='Dish Network'
#a1 file is ga1
#ot file is otshowpath, show is otshownames1

####it show information: leave it blank if no show falls into the category
###only work for 2 to 4 
numberofitshows=0
itseg_a1=['']
itseg_a1min=['']
itseg_a4=['']
itseg_a4min=['']
#otshowpath=hbo
#hbomvpd


###otshownames:
#otshowpath1
otshownames1=['HGTV _PROPERTY BROTHERS (1 HR) ','HGTV _FLIP OR FLOP             ','HGTV _MY LOTTERY DREAM HOME    ','HGTV _PROP BROS BUY AND SELL   ','HGTV _TINY HOUSE HUNTERS       ','HGTV _HOUSE HUNTERS RENOVATION ','HGTV _SP FIXER UPPER           ','HGTV _HOME TOWN                ','DIY  _TINY HOUSE BIG LIVING    ']
#otshowpath2
otshownames2=['']
#otshowpath3
otshownames3=['']
#otshowpath4
otshownames4=['']
#otshowpath5
otshownames5=['']
#otshowpath6
otshownames6=['']

otshownames = {1:otshownames1,
               2:otshownames2,
               3:otshownames3,
               4:otshownames4,
               5:otshownames5,
               6:otshownames6}

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
#quads=pd.read_csv(quadspath,dtype={'HHID':'object','PersonID':'object'})

#df=pd.merge(quads,demo,on=['HHID','PersonID'],how='right')
#df['Quads'].fillna(0,inplace=True)
df = demo.copy()
df=pd.merge(df,intab,on=['HHID','PersonID'],how='left')

#viewership=pd.read_csv(viewershippath,dtype={'HHID':'object','PersonID':'object'})
#df=pd.merge(df,viewership,on=['HHID','PersonID'],how='outer')

df['HHID_PersonID']=df['HHID']+'_'+df['PersonID']


dfga1 = pd.read_csv(ga1,dtype={'HHID':'object','PersonID':'object'})
dfga1['HHID'] = dfga1['HHID'].apply(lambda x:x.zfill(7))
dfga1['PersonID'] = dfga1['PersonID'].apply(lambda x:x.zfill(2))
dfga1['HHID_PersonID']=dfga1['HHID']+'_'+dfga1['PersonID']
#dfga3 = dfga3[dfga3['Totalmins']>=50]
dfga1 = dfga1[['HHID_PersonID']].drop_duplicates()
dfga1['ga1'] = 1

 
#viewership['HHID_PersonID']=viewership['HHID']+'_'+viewership['PersonID']
ITshows=pd.read_csv(itshowpath)


ITshows['A1mins']=0
for i in itseg_a1min:
   # ITshows[i]=np.where(ITshows[i]>0,1,0)
    ITshows['A1mins']=ITshows['A1mins']+ITshows[i]

ITshows['A4mins']=0
for i in itseg_a4min:
    ITshows['A4mins']=ITshows['A4mins']+ITshows[i]



ITshows=ITshows[['HHID_PersonID', 'A1mins','A4mins']]
ITshows=ITshows.drop_duplicates('HHID_PersonID')


df=pd.merge(dfga1,df,on='HHID_PersonID',how='right')
df=pd.merge(ITshows,df,on='HHID_PersonID',how='right')

otshownames = otshownames1 + otshownames2 +otshownames3
ot1 = pd.read_csv(otshowpath)
ot1['ot1'] = 0
for a in otshownames:
    if a in ot1.columns:
        ot1['ot1'] = ot1['ot1'] + ot1[a]
ot1 = ot1[['HHID_PersonID','ot1']]
ot1 = ot1[ot1['ot1']>0]
ot1 = ot1.drop_duplicates('HHID_PersonID')

ot2 = pd.DataFrame()
for i in otshowpath1:
    dfot = pd.read_csv(i)
    dfot['ot2'] = 0
    for a in otshownames:
        if a in dfot.columns:
            dfot['ot2'] = dfot['ot2'] + dfot[a]
    dfot = dfot[['HHID_PersonID','ot2']]
    dfot = dfot[dfot['ot2']>0]
    ot2 = ot2.append(dfot,ignore_index = True)
ot2 = ot2.drop_duplicates('HHID_PersonID')
ot = pd.merge(ot1,ot2,on = 'HHID_PersonID',how = 'outer' )
ot['ot'] = 1
ot = ot[['HHID_PersonID','ot']]
ot = ot.drop_duplicates('HHID_PersonID')
 
      

df=pd.merge(ot,df,on='HHID_PersonID',how='right')
df.fillna(0,inplace=True)

#quads = quads[quads['Cluster']=='Core_Occ']
#ITshows = ITshows[ITshows['Cluster']!=7]


df['finalsegment']=np.where((df['A1mins']>50)&(df['MVPD'].isin(['DirecTV','Dish Network'])),'A1',
                            np.where((df['A1mins']>50),'A2',
                            np.where((df['ga3']>0),'A3',
                            np.where((df['A4mins']>100)&(df['MVPD'].isin(['DirecTV','Dish Network'])),'A4',
                            np.where((df['A4mins']>100),'A5',
                            np.where((df['ot']>0),'ot','Other'))))))

print(df[['finalsegment','HHID_PersonID']].groupby('finalsegment').count())
df.to_csv("AudienceSizing_bellevue_300.csv",index=False)
print(datetime.datetime.now())

#test=pd.merge(OTshows2,df,on='HHID_PersonID')
#df['test']=np.where(df['BroadcastOT']>0,1,0)
#df.groupby('test').count()
