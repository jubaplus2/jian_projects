## -*- coding: utf-8 -*-
"""
Created on Mon Jun  6 15:02:11 2016

@author: Jubaplus 
"""
import datetime
print(datetime.datetime.now())
import pandas as pd

#####update the direcotry--all output files will be saved in this directory
foldername='/Users/jubaplus1/Desktop/Sundance/Lair/Sizing'
personintab='/Users/jubaplus1/Desktop/Sundance/Lair/Sizing/personintab_160101_170709.csv'
demofilepath='/Users/jubaplus1/Desktop/A&E/Dance Mom/Sizing/Demographicpersonlevel_160101_170611.csv'
filea2='/Users/jubaplus1/Desktop/Sundance/Lair/Sizing/sund_l3m_0709_100mins.csv'
filea4='/Users/jubaplus1/Desktop/Sundance/Lair/Sizing/sund_l12m_0709_100mins.csv'
quadspath='/Users/jubaplus1/Desktop/Sundance/Lair/Sizing/rectify_l12m.csv'
viewershippath=''
itshowpath='/Users/jubaplus1/Desktop/Sundance/Lair/LAIR_showaffnity_cluster_code.csv'

otshwopathmins=['/Users/jubaplus1/Desktop/Sundance/Lair/OT/Cable/programmin.csv']
bcastotindicator = 'T'
otshowpathfreq = ['/Users/jubaplus1/Desktop/Sundance/Lair/OT/Cable/programfreq.csv']

####put startdate and enddate for the project here:
Startdate=20160709
Enddate=20170709
gender = ['F','M']
minage = 25
maxage = 54

#a1 = file: quadspath, mins>100
#a2 = filea2, w25-54
#a3 = rest of filea2
#a4 = filea4, only w25-54
#a5 = file: itshowpath. shows in itseg_a5, any shows, mins>100
#a6 = file: itshowpath. shows in itseg_a6, any shows, mins>100
#ot = file: otshowpathfreq and itshowpath. shows in otshownames1 and otseg, any shows

####it show information: leave it blank if no show falls into the category
###only work for 2 to 4 
numberofitshows=6
itseg_a5=['Totalfreq362404','Totalfreq413007']
itseg_a5min=['Totalmins362404','Totalmins413007']
itseg_a6=['Totalfreq411324','Totalfreq415428']
itseg_a6min=['Totalmins411324','Totalmins415428']
otseg=['Totalfreq403308']
otseg_min=['Totalmins403308']
###otshownames:
#otshowpath1
otshownames1=['USA  _CSI                      ','USA  _NCIS: LOS ANGELES        ','HIST _VIKINGS                  ','USA  _SHOOTER                  ','AMC  _BETTER CALL SAUL         ','FX   _LEGION                   ','ID   _PEOPLE MAG INVESTIGATES  ','STZP _BLACK SAILS 4            ','ENT  _ARRANGEMENT              ','TNT  _MAJOR CRIMES             ']

otshownames = {1:otshownames1
               }
#a1otshows=['']

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
quads = quads[quads['Totalmins']>100]

df=pd.merge(quads,demo,on=['HHID','PersonID'],how='right')
df['Quads'].fillna(0,inplace=True)

#df = demo.copy()
df=pd.merge(df,intab,on=['HHID','PersonID'],how='left')

#viewership=pd.read_csv(viewershippath,dtype={'HHID':'object','PersonID':'object'})
#df=pd.merge(df,viewership,on=['HHID','PersonID'],how='outer')

df['HHID_PersonID']=df['HHID']+'_'+df['PersonID']

filea2shows=pd.read_csv(filea2)
filea2shows['A2'] = 1
filea2shows = filea2shows[['HHID_PersonID','A2']].drop_duplicates()
df=pd.merge(filea2shows,df,on='HHID_PersonID',how='right')

filea4shows=pd.read_csv(filea4)
filea4shows = filea4shows[filea4shows['Totalmins']>200]
filea4shows['A4'] = 1
filea4shows = filea4shows[['HHID_PersonID','A4']].drop_duplicates()
df=pd.merge(filea4shows,df,on='HHID_PersonID',how='right')

#viewership['HHID_PersonID']=viewership['HHID']+'_'+viewership['PersonID']
ITshows=pd.read_csv(itshowpath)
#ITshows=pd.merge(ITshows,viewership,on='HHID_PersonID',how='left')
ITshows['otit']=0
for i in otseg:
    ITshows['otit']=ITshows['otit']+ITshows[i]

ITshows['A5mins']=0
for i in itseg_a5min:
    ITshows['A5mins']=ITshows['A5mins']+ITshows[i]
 
ITshows['A6mins']=0
for i in itseg_a6min:
    ITshows['A6mins']=ITshows['A6mins']+ITshows[i]
    
ITshows=ITshows[['HHID_PersonID','otit', 'A5mins','A6mins','Cluster','ShiftedViewindicatorallprogram']]
ITshows.columns = ['HHID_PersonID', 'otit','A5mins','A6mins','Cluster_IT','ShiftedViewindicatorallprogram']
ITshows=ITshows.drop_duplicates('HHID_PersonID')

df=pd.merge(ITshows,df,on='HHID_PersonID',how='right')


dfotall = pd.DataFrame()
for i in otshwopathmins:
    dfot = pd.read_csv(i)
    dfot['ot'] = 0
    for a in otshownames1:
        if a in dfot.columns:
            dfot['ot'] = dfot['ot'] + dfot[a]
    dfot = dfot[['HHID_PersonID','ot']]
    dfot = dfot[(dfot['ot']>0)]
    dfotall = dfotall.append(dfot,ignore_index = True)
    
dfotall = dfotall.groupby('HHID_PersonID').sum()
dfotall.reset_index(inplace = True)

df=pd.merge(dfotall,df,on='HHID_PersonID',how='right')
df.fillna(0,inplace=True)

#quads = quads[quads['Cluster']=='Core_Occ']
#ITshows = ITshows[ITshows['Cluster']!=7]
df['ot'] = df['ot'] + df['otit']

df['finalsegment']=np.where((df['Quads']>0),'A1',
                            np.where((df['A2']>0)&(df['Gender'] == 'F'),'A2',
                            np.where((df['A2']>0),'A3',
                            np.where((df['A4']>0)&(df['Gender'] == 'F'),'A4',
                            np.where((df['A5mins']>100)&(df['Age']>34),'A5',
                            np.where((df['A6mins']>100)&(df['Age']>34),'A6',
                            np.where((df['ot']>0),'ot','Other')))))))

print(df[['finalsegment','HHID_PersonID']].groupby('finalsegment').count())
df.to_csv("AudienceSizing_lair_0807.csv",index=False)
print(datetime.datetime.now())

#test=pd.merge(OTshows2,df,on='HHID_PersonID')
#df['test']=np.where(df['BroadcastOT']>0,1,0)
#df.groupby('test').count()
