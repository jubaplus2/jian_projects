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
itshowpath='/Users/jubaplus1/Desktop/Z living/GoodFood/goodfood_showaffinity_cluster_code.csv'
otshowpath='/Users/jubaplus1/Desktop/Z living/GoodFood/OT/programfreq.csv'
bcastotindicator='F'
otshowpath1 = ['']
ga1='/Users/jubaplus1/Desktop/Z living/Sizing/zliving_goodfood_l12m.csv'

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
otshownames1=['FOOD _BEAT B.FLAY              ','TRAV _BF: DELICIOUS DESTINATION','FOOD _PIONEER WOMAN            ','FOOD _GUYS GROCERY GAMES       ','TRAV _FOOD PARADISES           ','FOOD _COOKS VS. CONS           ','FOOD _GINORMOUS FOOD           ','FOOD _FARMHOUSE RULES          ','FOOD _CHOPPED GRILL MASTERS    ','CC   _BURGERS, BREW & QUE      ','FOOD _IRON CHEF AMERICA        ','TRAV _BIZARRE FOODS AMERICA    ','FOOD _TEXAS CAKE HOUSE         ','FOOD _GUYS FAMILY ROAD TRIP    ','FOOD _GIADA AT HOME            ']
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


df=pd.merge(dfga1,df,on='HHID_PersonID',how='right')

ot1 = pd.read_csv(otshowpath)
ot1['ot1'] = 0
for a in otshownames1:
    if a in ot1.columns:
        ot1['ot1'] = ot1['ot1'] + ot1[a]
ot1 = ot1[['HHID_PersonID','ot1']]
ot1 = ot1[ot1['ot1']>0]
ot1 = ot1.drop_duplicates('HHID_PersonID')
     

df=pd.merge(ot1,df,on='HHID_PersonID',how='right')
df.fillna(0,inplace=True)

df = df[df['MVPD'].isin(['DirecTV','Dish Network'])]
#quads = quads[quads['Cluster']=='Core_Occ']
#ITshows = ITshows[ITshows['Cluster']!=7]


df['finalsegment']=np.where(df['ga1']>0,'A1',
                            np.where((df['ot1']>0),'ot','Other'))

print(df[['finalsegment','HHID_PersonID']].groupby('finalsegment').count())
df.to_csv("AudienceSizing_goodfood.csv",index=False)
print(datetime.datetime.now())

#test=pd.merge(OTshows2,df,on='HHID_PersonID')
#df['test']=np.where(df['BroadcastOT']>0,1,0)
#df.groupby('test').count()
