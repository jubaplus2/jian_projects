## -*- coding: utf-8 -*-

"""
Created on Mon Jun  6 15:02:11 2016
@author: Jubaplus 
"""

import datetime
print(datetime.datetime.now())

#####update the direcotry--all output files will be saved in this directory
foldername='/Users/jubaplus1/Desktop/GSN/America Says/Sizing/'
personintab='/Users/jubaplus1/Desktop/TV1/Down for Whatever/Sizing/personintab_160101_180422.csv'
demofilepath='/Users/jubaplus1/Desktop/TV1/Down for Whatever/Sizing/Demographicpersonlevel_160101_180422.csv'
quadspath=''
viewershippath=''
itshowpath='/Users/jubaplus1/Desktop/GSN/America Says/AmericaSays_showaffnity_cluster.csv'
otshowpath='/Users/jubaplus1/Desktop/GSN/America Says/OT/GSN/programfreq.csv'
bcastotindicator='T'
otshowpath1 = ['/Users/jubaplus1/Desktop/GSN/America Says/OT/GSN/programfreq.csv',
               '/Users/jubaplus1/Desktop/GSN/America Says/OT/Broadcast/programfreq.csv',
               '/Users/jubaplus1/Desktop/GSN/America Says/OT/Cable/programfreq.csv']

ga1='/Users/jubaplus1/Desktop/GSN/America Says/Sizing/gsn_a1a2.csv'
ga3='/Users/jubaplus1/Desktop/GSN/America Says/Sizing/gsn_a3a4.csv'
ga5='/Users/jubaplus1/Desktop/GSN/America Says/Sizing/gsn_l12m_422.csv'

####put startdate and enddate for the project here:
Startdate=20170422
Enddate=20180422

gender = ['F']
minage = 25
maxage = 54

####it show information: leave it blank if no show falls into the category
###only work for 2 to 4 

numberofitshows=0
itseg_a1=['']
itseg_a1min=['']
itseg_a3=['']
itseg_a3min=['']

#otshowpath=hbo
#hbomvpd

###otshownames:
#otshowpath1
otshownames1=['GSN  _WWTB MILLIONAIRE 30 MIN  ','GSN  _BAGGAGE                  ','GSN  _CHAIN REACTION (GSN)     ','GSN  _CARD SHARKS              ','GSN  _$25,000 PYRAMID          ','ABC  _AMER FUNN HOME VIDEOS    ','NBC  _ELLENS GAME OF GAMES     ','UP   _AFV                      ','USA  _ANW: NINJA VS. NINJA     ','TRU  _HACK MY LIFE             ','ABC  _CHEW, THE                ','CBS  _LUCKY DOG                ','CBS  _LUCKY DOG 2              ','LIF  _BRING IT!                ','FOX  _SHOWTIME AT THE APOLLO   ','CW   _ROBERT IRVINE SHOW       ']
#otshowpath2
otshownames2=['CMT  _MOM                      ','LIF  _FIRST 48, THE            ','WETV _CSI MIAMI                ','MTV2 _MY WIFE AND KIDS         ','CBS  _YOUNG AND THE RESTLESS   ','ABC  _GENERAL HOSPITAL         ','CBS  _BOLD AND THE BEAUTIFUL   ','FOX  _FOUR, THE                ','ION  _LAW & ORDER: SVU SAT 5P  ','TRU  _IMPRACTICAL JOKERS       ','TVL  _EVERYBODY LOVES RAYMOND  ','OWN  _DR. PHIL                 ','ID   _HOMICIDE HUNTER LT JOE KE','HGTV _PROP BROS BUY AND SELL   ']
#otshowpath3
#otshowpath4
otshownames4=['']
#otshowpath5
otshownames5=['']
#otshowpath6
otshownames6=['']

#otshownames = {1:otshownames1,
         #      2:otshownames2,
          #     3:otshownames3,
            #   4:otshownames4,
             #  5:otshownames5,
              # 6:otshownames6}

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

sum(personintab['PersonIntab'])

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
dfga1 = dfga1[dfga1['Totalmins']>=200]
dfga1 = dfga1[['HHID_PersonID']].drop_duplicates()
dfga1['ga1'] = 1

dfga3 = pd.read_csv(ga3,dtype={'HHID':'object','PersonID':'object'})
dfga3['HHID'] = dfga3['HHID'].apply(lambda x:x.zfill(7))
dfga3['PersonID'] = dfga3['PersonID'].apply(lambda x:x.zfill(2))
dfga3['HHID_PersonID']=dfga3['HHID']+'_'+dfga3['PersonID']
dfga3 = dfga3[dfga3['Totalmins']>=400]
dfga3 = dfga3[['HHID_PersonID']].drop_duplicates()
dfga3['ga3'] = 1

dfga5 = pd.read_csv(ga5,dtype={'HHID':'object','PersonID':'object'})
dfga5['HHID'] = dfga5['HHID'].apply(lambda x:x.zfill(7))
dfga5['PersonID'] = dfga5['PersonID'].apply(lambda x:x.zfill(2))
dfga5['HHID_PersonID']=dfga5['HHID']+'_'+dfga5['PersonID']
dfga5 = dfga5[dfga5['Totalmins']>=100]
dfga5 = dfga5[['HHID_PersonID','Totalmins']].drop_duplicates()
dfga5['ga5'] = 1

df=pd.merge(dfga1,df,on='HHID_PersonID',how='right')
df=pd.merge(dfga3,df,on='HHID_PersonID',how='right')
df=pd.merge(dfga5,df,on='HHID_PersonID',how='right')



ot1 = pd.DataFrame()
for i in otshowpath1:

    dfot = pd.read_csv(i)

    dfot['ot1'] = 0

    for a in otshownames2:

        if a in dfot.columns:

            dfot['ot1'] = dfot['ot1'] + dfot[a]

    dfot = dfot[['HHID_PersonID','ot1']]

    dfot = dfot[dfot['ot1']>0]

    ot1 = ot1.append(dfot,ignore_index = True)
    
    
ot2 = pd.DataFrame()

for i in otshowpath1:
    dfot = pd.read_csv(i)
    dfot['ot2'] = 0

    for a in otshownames1:

        if a in dfot.columns:

            dfot['ot2'] = dfot['ot2'] + dfot[a]

    dfot = dfot[['HHID_PersonID','ot2']]

    dfot = dfot[dfot['ot2']>0]

    ot2 = ot2.append(dfot,ignore_index = True)

ot1 = ot1.drop_duplicates('HHID_PersonID')
ot2 = ot2.drop_duplicates('HHID_PersonID')




ot = pd.merge(ot1,ot2,on = 'HHID_PersonID',how = 'outer' )

ot = ot.drop_duplicates('HHID_PersonID')

df=pd.merge(ot,df,on='HHID_PersonID',how='right')
df.fillna(0,inplace=True)

#quads = quads[quads['Cluster']=='Core_Occ']
#ITshows = ITshows[ITshows['Cluster']!=7]


#df['finalsegment']=np.where((df['ga1']==1)&(df['MVPD'].isin(['DirecTV','Dish Network'])),'A1',
 #                           np.where((df['ga1']==1),'A2',
  #                          np.where((df['ga3']==1)&(df['MVPD'].isin(['DirecTV','Dish Network'])),'A3',
   #                         np.where((df['ga3']==1),'A4',
    #                        np.where((df['ga5']==1),'A5',
     #                       np.where((df['ot1']>0),'ot1',
      #                      np.where((df['ot2']>0),'ot2','Other')))))))

df['finalsegment']=np.where((df['ga1']==1)&(df['MVPD'].isin(['DirecTV','Dish Network']))&(df['LPMCode'].isin(['101','104','105','106','110','111','112','117','124','139','202','218'])),'A1',
                            np.where((df['ga1']==1)&(df['LPMCode'].isin(['101','104','105','106','110','111','112','117','124','139','202','218'])),'A2',
                            np.where((df['ga3']==1)&(df['MVPD'].isin(['DirecTV','Dish Network']))&(df['LPMCode'].isin(['101','104','105','106','110','111','112','117','124','139','202','218'])),'A3',
                            np.where((df['ga3']==1)&(df['LPMCode'].isin(['101','104','105','106','110','111','112','117','124','139','202','218'])),'A4',
                            np.where((df['ga5']==1)&(df['LPMCode'].isin(['101','104','105','106','110','111','112','117','124','139','202','218'])),'A5',
                            np.where((df['ot1']>0)&(df['LPMCode'].isin(['101','104','105','106','110','111','112','117','124','139','202','218'])),'ot1',
                            np.where((df['ot2']>0)&(df['LPMCode'].isin(['101','104','105','106','110','111','112','117','124','139','202','218'])),'ot2','Other')))))))

print(df[['finalsegment','HHID_PersonID']].groupby('finalsegment').count())
df.to_csv("AudienceSizing_AS_241_DMA_new.csv",index=False)
print(datetime.datetime.now())

#test=pd.merge(OTshows2,df,on='HHID_PersonID')
#df['test']=np.where(df['BroadcastOT']>0,1,0)
#df.groupby('test').count()

df['ot2'].unique()

ot2
