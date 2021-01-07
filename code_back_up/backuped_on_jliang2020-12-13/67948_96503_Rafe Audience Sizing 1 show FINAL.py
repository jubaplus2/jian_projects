## -*- coding: utf-8 -*-
"""
Created on Mon Jun  6 15:02:11 2016

@author: Jubaplus 
"""
import datetime
print(datetime.datetime.now())

#####update the direcotry--all output files will be saved in this directory
foldername='/Users/jubaplus1/Desktop/Z Living'
personintab='/Users/jubaplus1/Desktop/Z Living/personintab_160101_170416.csv'
demofilepath='/Users/jubaplus1/Desktop/Z Living/Demographicpersonlevel_160101_170416.csv'
quadspath=''
viewershippath=''
itshowpath='/Users/jubaplus1/Desktop/Z Living/Z-living new_cluster.csv'
otshowpath=''
bcastotindicator='F'
otshowpath2=''
otshowpath4=''

####put startdate and enddate for the project here:
Startdate=20150526
Enddate=20160817

gender = ['F']
minage = 30
maxage = 54
top10dma = [101,104,112,108,351,218,419,213,117,353]
non10dma = [462,110,420,407,105,134,111,124,209,139,106,403,128,223,202]

#b1=all three shows + top10 dma + mvpd=dish
#b2=any two shows + top 10 dma + mvpd=dish
#b3=all three shows + non-top10 dma + mvpd=dish
#b4=any two shows + non-top 10 dma + mvpd=dish

####it show information: leave it blank if no show falls into the category
###only work for 2 to 4 
numberofitshows=2
itseg_a1=['Totalfreq351491','Totalfreq826306','Totalfreq848177']
itseg_a2=['']
itseg_a3=['']
itseg_a4=['']
###otshownames:
otshownames1=['']
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
intab=intab[['HHID','PersonID','PersonIntab']]
intab=intab.groupby(['HHID','PersonID']).mean()
intab.reset_index(inplace=True)

demo=pd.read_csv(demofilepath,dtype={'HHID':'object','PersonID':'object'})
demo = demo[demo['Gender'].isin(gender)]
demo = demo[(demo['Age']>=minage)&(demo['Age']<=maxage)]



#quads=pd.read_csv(quadspath,dtype={'HHID':'object','PersonID':'object'})

#df = pd.merge(quads,demo,on=['HHID','PersonID'],how='right')
#df['Quads'].fillna(0,inplace=True)

df=pd.merge(demo,intab,on=['HHID','PersonID'],how='left')

#viewership=pd.read_csv(viewershippath,dtype={'HHID':'object','PersonID':'object'})

#df=pd.merge(df,viewership,on=['HHID','PersonID'],how='outer')

df['HHID_PersonID']=df['HHID']+'_'+df['PersonID']

ITshows=pd.read_csv(itshowpath)


ITshows['A1']=0
#'ShiftedViewindicatorallprogram'
for i in itseg_a1:
    ITshows[i]=np.where(ITshows[i]>0,1,0)
    ITshows['A1']=ITshows['A1']+ITshows[i]
 

ITshows=ITshows[['HHID_PersonID', 'A1','ShiftedViewindicatorallprogram']]

ITshows=ITshows.drop_duplicates('HHID_PersonID')

df=pd.merge(ITshows,df,on='HHID_PersonID',how='right')

df['finalsegment']=np.where((df['A1']>=1)&(df['LPMCode'].isin(top10dma))&(df['MVPD']=='Dish Network'),'A1',
                            np.where((df['A1']>=1)&(df['LPMCode'].isin(non10dma))&(df['MVPD']=='Dish Network'),'A2',
                            np.where((df['A1']>=1)&(df['MVPD']=='Dish Network'),'A3',
                            np.where((df['A1']>=1)&(df['LPMCode'].isin(non10dma))&(df['MVPD']=='Dish Network'),'A4',
                            'Other'))))

df.to_csv("AudienceSizing_rafe 1 show final.csv",index=False)
print(df[['finalsegment','PersonIntab']].groupby('finalsegment').sum())
print(datetime.datetime.now())
