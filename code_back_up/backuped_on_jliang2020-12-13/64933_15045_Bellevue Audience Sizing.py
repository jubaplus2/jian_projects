## -*- coding: utf-8 -*-
"""
Created on Mon Jun  6 15:02:11 2016

@author: Jubaplus 
"""
import datetime
print(datetime.datetime.now())


#####update the direcotry--all output files will be saved in this directory
foldername='/Users/jubaplus1/Desktop/WGNA/Bellevue-Jan/Sizing'
personintab='/Users/jubaplus1/Desktop/WGNA/Bellevue-Jan/Sizing/personintab_160101_170903.csv'
demofilepath='/Users/jubaplus1/Desktop/A&E/Criminal Movies/Oscar Pistorius/Sizing/Demographicpersonlevel_160101_170820.csv'
quadspath=''
viewershippath=''
itshowpath='/Users/jubaplus1/Desktop/WGNA/Bellevue-Jan/Bellevue_showaffinity_cluster.csv'
otshowpath='/Users/jubaplus1/Desktop/WGNA/Bellevue-Jan/Bellevue OT/Cable/Cable/programmin.csv'
bcastotindicator='T'
otshowpath1 = ['/Users/jubaplus1/Desktop/WGNA/Bellevue-Jan/Bellevue OT/Broadcast/programmin.csv'
,'/Users/jubaplus1/Desktop/WGNA/Bellevue-Jan/Bellevue OT/Cable/WGNA/programmin.csv']
ga3='/Users/jubaplus1/Desktop/WGNA/Bellevue-Jan/Sizing/WGNA_l6m.csv'

####put startdate and enddate for the project here:
Startdate=20170303
Enddate=20170903
 
gender = ['F','M']
minage = 25
maxage = 54

#a1 file is itshowpath, show is in itseg_a1 and mvpd='Dish Network' or 'DirecTV' and mins >50
#a2 file is itshowpath, show is in itseg_a1 and mins >50
#a3 file is ga3
#a4 file is itshowpath, show is in itseg_a4 and mvpd='Dish Network' or 'DirecTV' and mins >100
#a5 file is itshowpath, show is in itseg_a4 and mins >100
#ot file is otshowpath and otshowpath1, show is otshownames1-3

####it show information: leave it blank if no show falls into the category
###only work for 2 to 4 
numberofitshows=2
itseg_a1=['Totalfreq390072','Totalfreq389975']
itseg_a1min=['Totalmins390072','Totalmins389975']
itseg_a4=['Totalfreq314758','Totalfreq358375','Totalfreq331106','Totalfreq412109']
itseg_a4min=['Totalmins314758','Totalmins358375','Totalmins331106','Totalmins412109']
#otshowpath=hbo
#hbomvpd


###otshownames:
#otshowpath1
otshownames1=['TNT  _LAW & ORDER              ','USA  _NCIS                     ','WETV _CSI MIAMI                ','TNT  _HAWAII FIVE-0            ']
#otshowpath2
otshownames2=['CBS  _NCIS                     ','CW   _SUPERNATURAL             ','CBS  _ELEMENTARY               ','FOX  _BONES                    ','ION  _CRIMINAL MINDS MON 10P   ','ION  _CRIMINAL MINDS MON 8P    ','ION  _CRIMINAL MINDS TUE 9P    ','FOX  _LUCIFER                  ','ION  _CRIMINAL MINDS TUE 7P    ','ION  _LAW & ORDER: SVU SAT 3P  ']
#otshowpath3
otshownames3=['SUND _MASH                     ','SUND _LAW & ORDER              ','SUND _48 HOUR MASH MARATHON    ','SUND _MASH MARATHON            ','SUND _HAP AND LEONARD          ']
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


dfga3 = pd.read_csv(ga3,dtype={'HHID':'object','PersonID':'object'})
dfga3['HHID_PersonID']=dfga3['HHID']+'_'+dfga3['PersonID']
dfga3 = dfga3[dfga3['Totalmins']>=50]
dfga3 = dfga3[['HHID_PersonID']].drop_duplicates()
dfga3['ga3'] = 1


#viewership['HHID_PersonID']=viewership['HHID']+'_'+viewership['PersonID']
ITshows=pd.read_csv(itshowpath)
ITshows=pd.merge(ITshows,dfga6,on='HHID_PersonID',how='outer')

itseg_a1=['Totalfreq389185','Totalfreq383595','ga6_1','ga6_2']
itseg_a1min=['Totalmins389185','Totalmins383595','Totalmins_ga61','Totalmins_ga62']

ITshows['A6']=0
for i in itseg_a1:
    ITshows[i]=np.where(ITshows[i]>0,1,0)
    ITshows['A6']=ITshows['A6']+ITshows[i]

ITshows['A6mins']=0
for i in itseg_a1min:
    ITshows['A6mins']=ITshows['A6mins']+ITshows[i]



ITshows=ITshows[['HHID_PersonID', 'A6','A6mins']]
ITshows=ITshows.drop_duplicates('HHID_PersonID')


df=pd.merge(dfga3,df,on='HHID_PersonID',how='right')
df=pd.merge(ITshows,df,on='HHID_PersonID',how='right')


ot1 = pd.read_csv(otshowpath)
ot1['ot1'] = 0
for a in otshownames1:
        ot1['ot1'] = ot1['ot1'] + ot1[a]
ot1 = ot1[['HHID_PersonID','ot1']]
ot1 = ot1.drop_duplicates('HHID_PersonID')

ot2 = pd.read_csv(otshowpath1)
ot2['ot2'] = 0
for a in otshownames2:
        ot2['ot2'] = ot2['ot2'] + ot2[a]
ot2 = ot2[['HHID_PersonID','ot2']]
ot2 = ot2.drop_duplicates('HHID_PersonID')


        

df=pd.merge(ot1,df,on='HHID_PersonID',how='right')
df=pd.merge(ot2,df,on='HHID_PersonID',how='right')
df.fillna(0,inplace=True)

#quads = quads[quads['Cluster']=='Core_Occ']
#ITshows = ITshows[ITshows['Cluster']!=7]


df['finalsegment']=np.where((df['ga1']>0),'A1',
                            np.where(df['ga2']>0),'A2',
                            np.where((df['ga3']>0)&(df['MVPD'].isin(['DirecTV','Dish Network'])),'A3',
                            np.where((df['ga4']>0)&(df['MVPD'].isin(['DirecTV','Dish Network'])),'A4',
                            np.where((df['ga5']>0),'A5',
                            np.where((df['A6']>=2)&(df['A6mins']>100),'A6',
                            np.where((df['ga7']>0),'A7',
                            np.where((df['ga8']>0),'A8',
                            np.where((df['gc']>0),'A9',
                            np.where((df['ot1']>0)|(df['ot2']>0),'ot','Other')))))))))

print(df[['finalsegment','HHID_PersonID']].groupby('finalsegment').count())
df.to_csv("AudienceSizing_bellevue.csv",index=False)
print(datetime.datetime.now())

#test=pd.merge(OTshows2,df,on='HHID_PersonID')
#df['test']=np.where(df['BroadcastOT']>0,1,0)
#df.groupby('test').count()
