## -*- coding: utf-8 -*-
"""
Created on Mon Jun  6 15:02:11 2016

@author: Jubaplus 
"""
import datetime
print(datetime.datetime.now())

#####update the direcotry--all output files will be saved in this directory
foldername='/Users/jubaplus1/Desktop/TV1/Unsung/Sizing'
personintab='/Users/jubaplus1/Desktop/TV1/Unsung/Sizing/personintab_150104_170326.csv'
demofilepath='/Users/jubaplus1/Desktop/TV1/Unsung/Sizing/Demographicpersonlevel_150201_170402.csv'
quadspath='/Users/jubaplus1/Desktop/TV1/Unsung/Sizing/unsung_l2y.csv'
viewershippath=''
itshowpath='/Users/jubaplus1/Desktop/TV1/Unsung/Unsung_showaffinity_cluster.csv'
otshowpath='/Users/jubaplus1/Desktop/TV1/Unsung/Sizing/tv1_l12m_402.csv'
bcastotindicator='T'
otshowpath2='/Users/jubaplus1/Desktop/TV1/Unsung/OT/Cable/cable_programfreq.csv'
otshowpath4=''

####put startdate and enddate for the project here:
Startdate=20150402
Enddate=20170402

gender = ['F','M']
minage = 25
maxage = 54

#A1=quadspath
#A2=otshowpath+race=aa=1
#A3=itshowpath,itseg_a1,any show
#A4 part1=itshowpath,itseg_a2:Totalfreq841187,SHIFTED ONLY=2/3
#A4 part2=otshowpath2, otshownames1
#A4 any 2 of the 3 shows
#ot=otshowpath2,otshownames2

####it show information: leave it blank if no show falls into the category
###only work for 2 to 4 
numberofitshows=2
itseg_a1=['Totalfreq392374','Totalfreq412692']
itseg_a2=['Totalfreq841187']
itseg_a3=['']
itseg_a4=['']
###otshownames:
otshownames1=['BRVO _REAL HOUSEWIVES ATLANTA  ','OWN  _GREENLEAF                ']
otshownames2=['VH1  _JAMIE FOXX SHOW          ','VH1  _LOVE & HIP HOP HLLYWD 3  ','BET  _HOUSE OF PAYNE           ','MTV2 _MY WIFE AND KIDS         ','BRVO _MARRIED TO MEDICINE      ','VH1  _FRESH PRINCE OF BEL-AIR  ','OWN  _IF LOVING YOU IS WRONG   ','LIF  _LITTLE WOMEN ATL         ','OWN  _FOR BETTER OR WORSE      ','WGNA _IN THE HEAT OF THE NIGHT ']
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
intab=intab[['HHID','PersonID','PersonIntab']]
intab=intab.groupby(['HHID','PersonID']).mean()
intab.reset_index(inplace=True)

demo=pd.read_csv(demofilepath,dtype={'HHID':'object','PersonID':'object'})
demo = demo[demo['Gender'].isin(gender)]
demo = demo[(demo['Age']>=minage)&(demo['Age']<=maxage)]

quads=pd.read_csv(quadspath,dtype={'HHID':'object','PersonID':'object'})

df = pd.merge(quads,demo,on=['HHID','PersonID'],how='right')
df['Quads'].fillna(0,inplace=True)

df=pd.merge(df,intab,on=['HHID','PersonID'],how='left')

#viewership=pd.read_csv(viewershippath,dtype={'HHID':'object','PersonID':'object'})

#df=pd.merge(df,viewership,on=['HHID','PersonID'],how='outer')

df['HHID_PersonID']=df['HHID']+'_'+df['PersonID']

ITshows=pd.read_csv(itshowpath)

ITshows['A3']=0
for i in itseg_a1:
    ITshows['A3']=ITshows['A3']+ITshows[i]
    
ITshows['A4']=0
#'ShiftedViewindicatorallprogram'
for i in itseg_a2:
    ITshows[i]=np.where(ITshows[i]>0,1,0)
    ITshows['A4']=ITshows['A4']+ITshows[i]
ITshows['A4'] = np.where(ITshows['ShiftedViewindicatorallprogram']>=2,ITshows['A4'],0)    



ITshows=ITshows[['HHID_PersonID', 'A3', 'A4','ShiftedViewindicatorallprogram']]

ITshows=ITshows.drop_duplicates('HHID_PersonID')

df=pd.merge(ITshows,df,on='HHID_PersonID',how='right')


OTshows=pd.read_csv(otshowpath,dtype = 'str')
OTshows['HHID_PersonID']=OTshows['HHID']+'_'+OTshows['PersonID']
OTshows['A2']=1

OTshows2=pd.read_csv(otshowpath2)
OTshows2['A42']=0
for i in otshownames1:
    OTshows2[i]=np.where(OTshows2[i]>0,1,0)
    OTshows2['A42']=OTshows2['A42']+OTshows2[i]

OTshows2['CableOT']=0
for i in otshownames2:
    OTshows2['CableOT']=OTshows2['CableOT']+OTshows2[i]


OTshows=OTshows[['HHID_PersonID','A2']]
OTshows=OTshows.drop_duplicates('HHID_PersonID')
OTshows2=OTshows2[['HHID_PersonID','A42','CableOT']]
OTshows2=OTshows2.drop_duplicates('HHID_PersonID')



df=pd.merge(OTshows,df,on='HHID_PersonID',how='right')
df=pd.merge(OTshows2,df,on='HHID_PersonID',how='right')
df.fillna(0,inplace=True)

df['A4'] = df['A4']+df['A42']

df['finalsegment']=np.where((df['Quads']>0),'A1',
                            np.where((df['A2']>0)&((df['Race']==1)),'A2',
                            np.where(df['A3']>0,'A3',
                            np.where(df['A4']>=2,'A4',
                            np.where(df['CableOT']>0,'CableOT','Other')))))

df.to_csv("AudienceSizing_unsung.csv",index=False)
print(df[['finalsegment','PersonIntab']].groupby('finalsegment').sum())
print(datetime.datetime.now())
