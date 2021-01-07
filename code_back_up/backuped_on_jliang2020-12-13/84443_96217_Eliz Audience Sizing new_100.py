## -*- coding: utf-8 -*-
"""
Created on Mon Jun  6 15:02:11 2016

@author: Jubaplus 
"""
import datetime
print(datetime.datetime.now())


#####update the direcotry--all output files will be saved in this directory
foldername='/Users/jubaplus1/Desktop/A&E/Criminal Movies/Elizabeth Smart/Sizing'
personintab='/Users/jubaplus1/Desktop/A&E/Criminal Movies/Oscar Pistorius/Sizing/personintab_160101_170820.csv'
demofilepath='/Users/jubaplus1/Desktop/A&E/Criminal Movies/Oscar Pistorius/Sizing/Demographicpersonlevel_160101_170820.csv'
quadspath=''
viewershippath=''
hbomvpd=''
itshowpath='/Users/jubaplus1/Desktop/A&E/Criminal Movies/Elizabeth Smart/elizabeth_showaffinitynew_code.csv'
otshowpath='/Users/jubaplus1/Desktop/A&E/Criminal Movies/Elizabeth Smart/Elizabeth OT/Broadcast/Eli_Broadcast_programmin.csv'
bcastotindicator='T'
otshowpath1 = ['/Users/jubaplus1/Desktop/A&E/Criminal Movies/Elizabeth Smart/Elizabeth OT/Cable/HISTORY/programmin.csv'
,'/Users/jubaplus1/Desktop/A&E/Criminal Movies/Elizabeth Smart/Elizabeth OT/Cable/AEN/programmin.csv'
,'/Users/jubaplus1/Desktop/A&E/Criminal Movies/Elizabeth Smart/Elizabeth OT/Cable/Cable/Eli_Cable_programmin.csv'
,'/Users/jubaplus1/Desktop/A&E/Criminal Movies/Elizabeth Smart/Elizabeth OT/Cable/FYI/programmin.csv'
,'/Users/jubaplus1/Desktop/A&E/Criminal Movies/Elizabeth Smart/Elizabeth OT/Cable/LIF/programmin.csv'
,'/Users/jubaplus1/Desktop/A&E/Criminal Movies/Elizabeth Smart/Elizabeth OT/Cable/LMN/programmin.csv']
ga5='/Users/jubaplus1/Desktop/A&E/Criminal Movies/Oscar Pistorius/Sizing/hln_l12m.csv'


####put startdate and enddate for the project here:
Startdate=20160101
Enddate=20170803
 
gender = ['F']
minage = 25
maxage = 54


#a1 file is itshowpath, show is in itseg_a1, and age=40-54 and race=2 and mvpd='Dish Network' or 'DirecTV' and mins >30
#a2 file is itshowpath, show is in itseg_a2, and age=40-54 and race=2 and mvpd='Dish Network' or 'DirecTV' and mins >150
#a3 file is otshowpath1, show is in otshownames1, and age=40-54 and race=2 and mvpd='Dish Network' or 'DirecTV' and mins >150
#a4 file is otshowpath, show is in otshownames2, and age=40-54 and race=2 and mvpd='Dish Network' or 'DirecTV' and mins >150
#a5 file is ga5,and age=40-54 and race=2 and mvpd='Dish Network' or 'DirecTV' and mins >200
#a6 file is itshowpath, show is in itseg_a2, and age=40-54 and race=2 and mins >150
#a7 file is otshowpath1, show is in otshownames1, and age=40-54 and race=2 and mins >150
#a8 file is otshowpath, show is in otshownames2, and age=40-54 and race=2 and mins >150
#a9 file is ga5,and age=40-54 and race=2 and mins >200
#ot file is otshowpath and otshowpath1, show is otshownames1-otshownames6

####it show information: leave it blank if no show falls into the category
###only work for 2 to 4 
numberofitshows=4
itseg_a1=['Totalfreq261238','Totalfreq393342']
itseg_a1min=['Totalmins261238','Totalmins393342']
itseg_a2=['Totalfreq392780','Totalfreq393343','Totalfreq383772','Totalfreq252195','Totalfreq395373']
itseg_a2min=['Totalmins392780','Totalmins393343','Totalmins383772','Totalmins252195','Totalmins395373']
#otshowpath=hbo
#hbomvpd


###otshownames:
#otshowpath1
otshownames1=['OWN  _DATELINE ON OWN          ','HLN  _HOW IT REALLY HAPPENED   ','ID   _VANITY FAIR CONFIDENTIAL ','ID   _MURDER CHOSE ME          ','DISC _COOPERS TREASURE         ']
#otshowpath2
otshownames2=['CBS  _HUNTED                   ','CBS  _CODE BLACK               ','CBS  _HIDDEN HEROES            ']
#otshowpath3
otshownames3=['AEN  _DOG THE BOUNTY HUNTER    ','AEN  _NIGHTWATCH               ','AEN  _FIRST 48 MINI            ','']
#otshowpath4
otshownames4=['FYI  _DUCK DYNASTY             ']
#otshowpath5
otshownames5=['HIST _SWAMP PEOPLE             ','HIST _FORGED IN FIRE           ']
#otshowpath6
otshownames6=['LMN  _MARY KILLS PEOPLE        ','LMN  _BEYOND THE HEADLINES     ','LMN  _MY CRAZY EX              ']

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
#quads['HHID'] = quads['HHID'].apply(lambda x:x.zfill(7))
#quads['PersonID'] = quads['PersonID'].apply(lambda x:x.zfill(2))


#df=pd.merge(quads,demo,on=['HHID','PersonID'],how='right')
#df['Quads'].fillna(0,inplace=True)
df = demo.copy()
df=pd.merge(df,intab,on=['HHID','PersonID'],how='left')

#viewership=pd.read_csv(viewershippath,dtype={'HHID':'object','PersonID':'object'})
#df=pd.merge(df,viewership,on=['HHID','PersonID'],how='outer')

df['HHID_PersonID']=df['HHID']+'_'+df['PersonID']

dfga5 = pd.read_csv(ga5,dtype={'HHID':'object','PersonID':'object'})
#dfga5['HHID_PersonID']=dfga5['HHID']+'_'+dfga5['PersonID']
dfga5 = dfga5[dfga5['Totalmins']> 100]
dfga5 = dfga5[['HHID_PersonID']].drop_duplicates()
dfga5['ga5'] = 1


#viewership['HHID_PersonID']=viewership['HHID']+'_'+viewership['PersonID']
ITshows=pd.read_csv(itshowpath)

ITshows['A2']=0
for i in itseg_a2:
    ITshows[i]=np.where(ITshows[i]>0,1,0)
    ITshows['A2']=ITshows['A2']+ITshows[i]

ITshows['A2mins']=0
for i in itseg_a2min:
    ITshows['A2mins']=ITshows['A2mins']+ITshows[i]

ITshows['A1']=0
for i in itseg_a1:
    ITshows[i]=np.where(ITshows[i]>0,1,0)
    ITshows['A1']=ITshows['A1']+ITshows[i]

ITshows['A1mins']=0
for i in itseg_a1min:
    ITshows['A1mins']=ITshows['A1mins']+ITshows[i]

ITshows=ITshows[['HHID_PersonID', 'A1','A1mins', 'A2','A2mins']]
ITshows=ITshows.drop_duplicates('HHID_PersonID')

df=pd.merge(dfga5,df,on='HHID_PersonID',how='right')
df=pd.merge(ITshows,df,on='HHID_PersonID',how='right')


ot4 = pd.read_csv(otshowpath)
ot4['ot4'] = 0
for a in otshownames2:
        ot4['ot4'] = ot4['ot4'] + ot4[a]
ot4 = ot4[['HHID_PersonID','ot4']]
ot4 = ot4.drop_duplicates('HHID_PersonID')

otshownamesconcat = otshownames1 +otshownames2 +otshownames3+\
                    otshownames4 +otshownames5 +otshownames6

ot2 = pd.read_csv(otshowpath)
ot2['ot2'] = 0
for a in otshownamesconcat:
    if a in ot2.columns:
        ot2['ot2'] = ot2['ot2'] + ot2[a]
ot2 = ot2[['HHID_PersonID','ot2']]
ot2 = ot2.drop_duplicates('HHID_PersonID')

otall = pd.DataFrame()
for i in otshowpath1:
    ot3 = pd.read_csv(i)
    ot3['ot3'] = 0
    for a in otshownamesconcat:
        if a in ot3.columns:
            ot3['ot3'] = ot3['ot3'] + ot3[a]
    ot3 = ot3[['HHID_PersonID','ot3']]
    otall = otall.append(ot3,ignore_index = True)
    otall = otall.groupby('HHID_PersonID').sum()
    otall.reset_index(inplace = True)

otall = pd.merge(ot2,otall,on='HHID_PersonID',how='outer')
otall.fillna(0,inplace = True)
otall['ot'] = otall['ot2']+otall['ot3']
otall = otall[['HHID_PersonID','ot']]
otall = otall.drop_duplicates('HHID_PersonID')


otall3 = pd.DataFrame()
for i in otshowpath1:
    ot3 = pd.read_csv(i)
    ot3['ot3'] = 0
    for a in otshownames1:
        if a in ot3.columns:
            ot3['ot3'] = ot3['ot3'] + ot3[a]
    ot3 = ot3[['HHID_PersonID','ot3']]
    otall3 = otall3.append(ot3,ignore_index = True)
    otall3 = otall3.groupby('HHID_PersonID').sum()
    otall3.reset_index(inplace = True)
    
    
df=pd.merge(otall3,df,on='HHID_PersonID',how='right')
df=pd.merge(ot4,df,on='HHID_PersonID',how='right')
df=pd.merge(otall,df,on='HHID_PersonID',how='right')

df.fillna(0,inplace=True)

#quads = quads[quads['Cluster']=='Core_Occ']
#ITshows = ITshows[ITshows['Cluster']!=7]


df['finalsegment']=np.where((df['A1mins']>30)&(df['MVPD'].isin(['DirecTV','Dish Network']))&(df['Age'].isin(range(40,55)))&(df['Race']==2),'A1',
                            np.where((df['A2mins']>100)&(df['MVPD'].isin(['DirecTV','Dish Network']))&(df['Age'].isin(range(40,55)))&(df['Race']==2),'A2',
                            np.where((df['ot3']>100)&(df['MVPD'].isin(['DirecTV','Dish Network']))&(df['Age'].isin(range(40,55)))&(df['Race']==2),'A3',
                            np.where((df['ot4']>100)&(df['MVPD'].isin(['DirecTV','Dish Network']))&(df['Age'].isin(range(40,55)))&(df['Race']==2),'A4',
                            np.where((df['ga5']>0)&(df['MVPD'].isin(['DirecTV','Dish Network']))&(df['Age'].isin(range(40,55)))&(df['Race']==2),'A5',
                            np.where((df['A2mins']>150)&(df['Age'].isin(range(40,55)))&(df['Race']==2),'A6',
                            np.where((df['ot3']>150)&(df['Age'].isin(range(40,55)))&(df['Race']==2),'A7',
                            np.where((df['ot4']>150)&(df['Age'].isin(range(40,55)))&(df['Race']==2),'A8',
                            np.where((df['ga5']>0)&(df['Age'].isin(range(40,55)))&(df['Race']==2),'A9',
                            np.where((df['ot']>0),'ot','Other'))))))))))

print(df[['finalsegment','HHID_PersonID']].groupby('finalsegment').count())
df.to_csv("AudienceSizing_eliz_100.csv",index=False)
print(datetime.datetime.now())

#test=pd.merge(OTshows2,df,on='HHID_PersonID')
#df['test']=np.where(df['BroadcastOT']>0,1,0)
#df.groupby('test').count()
