## -*- coding: utf-8 -*-
"""
Created on Mon Jun  6 15:02:11 2016

@author: Jubaplus 
"""
import datetime
print(datetime.datetime.now())
import pandas as pd


#####update the direcotry--all output files will be saved in this directory
foldername='/Users/jubaplus1/Desktop/A&E/Growing up Supermodel/Sizing'
personintab='/Users/jubaplus1/Desktop/A&E/Dance Mom/Sizing/personintab_160101_170611.csv'
demofilepath='/Users/jubaplus1/Desktop/A&E/Dance Mom/Sizing/Demographicpersonlevel_160101_170611.csv'
filea1='/Users/jubaplus1/Desktop/A&E/Growing up Supermodel/Sizing/lif_a1_l6m.csv'
filea2='/Users/jubaplus1/Desktop/A&E/Growing up Supermodel/Sizing/lif_a2_l6m.csv'
quadspath=''
viewershippath=''
itshowpath='/Users/jubaplus1/Desktop/A&E/Growing up Supermodel/LIF_Supermodel_showaffinity_cluster_code.csv'
otshowpath=''
otshwopathmins=['/Users/jubaplus1/Desktop/A&E/Growing up Supermodel/OT/LIF/programmin.csv'
,'/Users/jubaplus1/Desktop/A&E/Growing up Supermodel/OT/Cable/programmin.csv']
bcastotindicator = 'T'
otshowpath1 = ['/Users/jubaplus1/Desktop/A&E/Growing up Supermodel/OT/LIF/programfreq.csv'
,'/Users/jubaplus1/Desktop/A&E/Growing up Supermodel/OT/Cable/programfreq.csv']

####put startdate and enddate for the project here:
Startdate=20160611
Enddate=20170611

gender = ['F']
minage = 25
maxage = 54

#a1 = file a1
#a2 = file a2
#a3 files: itshowpath and otshowpath1. Any of the shows ,sum of mins over 200, mvpd=dtv or dish
# mins of shows in otshowpath1, use file a3min1 and a3min2
#a4 file itseg_a4, mins itseg_a4min. Any of the shows ,sum of mins over 200, mvpd=dtv or dish
#a5 files: itshowpath and otshowpath1. Any of the shows ,sum of mins over 200, shifedcode=2 or 3
#a6 file itseg_a4, mins itseg_a4min. Any of the shows ,sum of mins over 200, shifedcode=2 or 3
#ot files: otshowpath1. shows in otshownames1

####it show information: leave it blank if no show falls into the category
###only work for 2 to 4 
numberofitshows=4
itseg_a3=['Totalfreq373656']
itseg_a3min=['Totalmins373656']
itseg_a3_program = ['373656']
itseg_a4=['Totalfreq415600','Totalfreq418956','Totalfreq363715','Totalfreq418606','Totalfreq397489']
itseg_a4min=['Totalmins415600','Totalmins418956','Totalmins363715','Totalmins418606','Totalmins397489']
itseg_a4_program = ['415600','418956','363715','418606','397489']
###otshownames:
#otshowpath1
otshownames1=['BRVO _REAL HOUSEWIVES ATLANTA  ','OXYG _SNAPPED                  ','BRVO _REAL HOUSEWIVES BEV HILLS','VH1  _LOVE & HIP HOP ATLANTA 6 ','USA  _CHRISLEY KNOWS BEST      ','BET  _MARTIN                   ','BRVO _WATCH WHAT HAPPENS LIVE  ','BET  _MEET THE BROWN           ','MTV  _TEEN MOM II SSN7B        ','ENT  _MARIAHS WORLD            ','LIF  _BRING IT!                ','LIF  _PROJECT RUNWAY           ','LIF  _RAP GAME                 ','LIF  _LITTLE WOMEN LA          ','LIF  _LW ATL: A LITTLE EXTRA   ','LIF  _DANCE MOMS               ']

otshownames = {1:otshownames1
               }
a3a5otshows=['ENT  _KEEPING UP KARDASHIANS   ','LIF  _PROJECT RUNWAY           ']

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

filea1shows=pd.read_csv(filea1)
filea1shows['A1'] = 1
filea1shows = filea1shows[['HHID_PersonID','A1']].drop_duplicates()
df=pd.merge(filea1shows,df,on='HHID_PersonID',how='right')

filea2shows=pd.read_csv(filea2)
filea2shows['A2'] = 1
filea2shows = filea2shows[['HHID_PersonID','A2']].drop_duplicates()
df=pd.merge(filea2shows,df,on='HHID_PersonID',how='right')

#viewership['HHID_PersonID']=viewership['HHID']+'_'+viewership['PersonID']
ITshows=pd.read_csv(itshowpath)
#ITshows=pd.merge(ITshows,viewership,on='HHID_PersonID',how='left')


ITshows['A3']=0
for i in itseg_a3:
    ITshows[i]=np.where(ITshows[i]>0,1,0)
    ITshows['A3']=ITshows['A3']+ITshows[i]

ITshows['A3mins']=0
for i in itseg_a3min:
    ITshows['A3mins']=ITshows['A3mins']+ITshows[i]


ITshows['A3Shifted'] = 0
for i in itseg_a3_program:
    ITshows['ShiftedViewindicator'+i]=np.where(ITshows['ShiftedViewindicator'+i]>1,1,0)
    ITshows['A3Shifted']=ITshows['A3Shifted']+ITshows['ShiftedViewindicator'+i]

ITshows['A4']=0
for i in itseg_a4:
    ITshows[i]=np.where(ITshows[i]>0,1,0)
    ITshows['A4']=ITshows['A4']+ITshows[i]

ITshows['A4mins']=0
for i in itseg_a4min:
    ITshows['A4mins']=ITshows['A4mins']+ITshows[i]

ITshows['A4Shifted'] = 0
for i in itseg_a4_program:
    ITshows['ShiftedViewindicator'+i]=np.where(ITshows['ShiftedViewindicator'+i]>1,1,0)
    ITshows['A4Shifted']=ITshows['A4Shifted']+ITshows['ShiftedViewindicator'+i]
    
ITshows=ITshows[['HHID_PersonID', 'A3','A4','A3mins','A4mins','A3Shifted','A4Shifted','Cluster','ShiftedViewindicatorallprogram']]
ITshows.columns = ['HHID_PersonID', 'A3','A4','A3mins','A4mins','A3Shifted','A4Shifted','Cluster_IT','ShiftedViewindicatorallprogram']
ITshows=ITshows.drop_duplicates('HHID_PersonID')

df=pd.merge(ITshows,df,on='HHID_PersonID',how='right')


OTmins = pd.DataFrame()
for i in otshwopathmins:
    dfot = pd.read_csv(i)
    dfot['a3otmins'] = 0
    for a in a3a5otshows:
        if a in dfot.columns:
            dfot['a3otmins'] = dfot['a3otmins'] + dfot[a]
    dfot = dfot[['HHID_PersonID','a3otmins']]
    dfot = dfot[dfot['a3otmins']>0]
    OTmins = OTmins.append(dfot,ignore_index = True)
    
OTmins = OTmins.groupby('HHID_PersonID').sum()
OTmins.reset_index(inplace = True)
df=pd.merge(OTmins,df,on='HHID_PersonID',how='right')
df.fillna(0,inplace=True)

dfotall = pd.DataFrame()
for i in otshowpath1:
    dfot = pd.read_csv(i)
    dfot['ot'] = 0
    dfot['a3ot'] = 0
    for a in otshownames1:
        if a in dfot.columns:
            dfot['ot'] = dfot['ot'] + dfot[a]
    for a in a3a5otshows:
        if a in dfot.columns:
            dfot['a3ot'] = dfot['a3ot'] + dfot[a]
    dfot = dfot[['HHID_PersonID','ot','a3ot']]
    dfot = dfot[(dfot['ot']>0)|(dfot['a3ot']>0)]
    dfotall = dfotall.append(dfot,ignore_index = True)
    
dfotall = dfotall.groupby('HHID_PersonID').sum()
dfotall.reset_index(inplace = True)

df=pd.merge(dfotall,df,on='HHID_PersonID',how='right')
df.fillna(0,inplace=True)

#quads = quads[quads['Cluster']=='Core_Occ']
#ITshows = ITshows[ITshows['Cluster']!=7]
df['A3'] = df['A3'] + df['a3ot']
df['A3mins'] = df['A3mins'] + df['a3otmins']

df['finalsegment']=np.where((df['A1']>0),'A1',
                            np.where((df['A2']>0),'A2',
                            np.where((df['A3']>0)&(df['A3mins']>200)&(df['MVPD'].isin(['DirecTV','Dish Network'])),'A3',
                            np.where((df['A4']>0)&(df['A4mins']>200)&(df['MVPD'].isin(['DirecTV','Dish Network'])),'A4',
                            np.where(((df['A3']>0)&(df['A3mins']>200)&(df['A3Shifted']>0))|((df['a3ot']>0)&(df['a3otmins']>200)),'A5',
                            np.where((df['A4']>0)&(df['A4mins']>200)&(df['A4Shifted']>0),'A6',
                            np.where((df['ot']>0),'ot','Other')))))))

print(df[['finalsegment','HHID_PersonID']].groupby('finalsegment').count())
df.to_csv("AudienceSizing_supermodel3.csv",index=False)
print(datetime.datetime.now())

#test=pd.merge(OTshows2,df,on='HHID_PersonID')
#df['test']=np.where(df['BroadcastOT']>0,1,0)
#df.groupby('test').count()
