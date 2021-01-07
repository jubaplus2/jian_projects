## -*- coding: utf-8 -*-
"""
Created on Mon Jun  6 15:02:11 2016

@author: Jubaplus 
"""
import datetime
print(datetime.datetime.now())

#####update the direcotry--all output files will be saved in this directory
foldername='/Users/jubaplus1/Desktop/A&E/Dance Mom/Sizing'
personintab='/Users/jubaplus1/Desktop/A&E/Dance Mom/Sizing/personintab_160101_170611.csv'
demofilepath='/Users/jubaplus1/Desktop/A&E/Dance Mom/Sizing/Demographicpersonlevel_160101_170611.csv'
quadspath='/Users/jubaplus1/Desktop/A&E/Dance Mom/Sizing/LIF_l12m_0611.csv'
viewershippath=''
itshowpath='/Users/jubaplus1/Desktop/A&E/Dance Mom/IT_showaffnity_cluster_code.csv'
otshowpath='/Users/jubaplus1/Desktop/A&E/Dance Mom/Sizing/DanceMom_Last_Season.csv'
bcastotindicator = 'T'
otshowpath1 = ['/Users/jubaplus1/Desktop/A&E/Dance Mom/OT/Cable/programfreq.csv'
,'/Users/jubaplus1/Desktop/A&E/Dance Mom/OT/LIF/programfreq.csv'
,'/Users/jubaplus1/Desktop/A&E/Dance Mom/OT/LMN/programfreq.csv']

####put startdate and enddate for the project here:
Startdate=20160613
Enddate=20170611

gender = ['F']
minage = 25
maxage = 54

#a1 file is otshowpath; and MVPD = 'Dish Network' or 'DirecTV'; and 'Mins'>100
#a2 file is otshowpath; balance of a1
#a3 file is quadspath and itshowpath. in quadspath, only 'Cluster'='Core_Occ', in itshowpath, show is itseg_a1, any one of the two shows. final a3 is overlap of quadspath and itshowpath
#a4 file is itshowpath. show is in itseg_a2, must watch both shows, and sum of itseg_a2min > 200 ,and MVPD = 'Dish Network' or 'DirecTV'
#a5 file is itshowpath. show is in itseg_a3, and itseg_a3min > 100 ,and MVPD = 'Dish Network' or 'DirecTV'
#a6 file is itshowpath. show is in itseg_a4, [Totalfreq320916 AND Totalfreq337744] or Totalfreq353846, and MVPD NOT = 'Dish Network' or 'DirecTV', and ShiftedViewindicator320916/ShiftedViewindicator337744/ShiftedViewindicator353846 = 2 or 3
#ot file is itshowpath and otshowpath1. in itshowpath, show is itseg_a5, in otshowpath1, show is otshownames. any of these shows

####it show information: leave it blank if no show falls into the category
###only work for 2 to 4 
numberofitshows=7
itseg_a1=['Totalfreq373522','Totalfreq420782']
itseg_a2=['Totalfreq320916','Totalfreq337744']
itseg_a2min=['Totalmins320916','Totalmins337744']
itseg_a3=['Totalfreq353846']
itseg_a3min=['Totalmins353846']
itseg_a41=['Totalfreq320916','Totalfreq337744']
itseg_a42 = ['Totalfreq353846']
itseg_a5=['Totalfreq404360','Totalfreq410223','Totalfreq410187'] 

###otshownames:
#otshowpath1
otshownames1=['BRVO _VANDERPUMP RULES         ','BRVO _WATCH WHAT HAPPENS LIVE  ','WETV _MARRIAGE BOOT CAMP: RS   ','BRVO _TIMBER CREEK LODGE       ','VH1  _AMERICAS TOP MODEL 23    ','BRVO _IMPOSTERS                ','TLC  _SISTER WIVES             ','BRVO _REAL HOUSEWIVES POTOMAC  ','BRVO _MARRIED TO MEDICINE      ','WETV _BRAXTON FAMILY VALUES    ','LIF  _LITTLE WOMEN ATL         ','LMN  _24 TO LIFE               ']

otshownames = {1:otshownames1
               }

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
quads=pd.read_csv(quadspath,dtype={'HHID':'object','PersonID':'object'})

df=pd.merge(quads,demo,on=['HHID','PersonID'],how='right')
df['Quads'].fillna(0,inplace=True)
df=pd.merge(df,intab,on=['HHID','PersonID'],how='left')

#viewership=pd.read_csv(viewershippath,dtype={'HHID':'object','PersonID':'object'})
#df=pd.merge(df,viewership,on=['HHID','PersonID'],how='outer')

df['HHID_PersonID']=df['HHID']+'_'+df['PersonID']

#viewership['HHID_PersonID']=viewership['HHID']+'_'+viewership['PersonID']
ITshows=pd.read_csv(itshowpath)
#ITshows=pd.merge(ITshows,viewership,on='HHID_PersonID',how='left')


ITshows['A1']=0
for i in itseg_a1:
    ITshows[i]=np.where(ITshows[i]>0,1,0)
    ITshows['A1']=ITshows['A1']+ITshows[i]

ITshows['A2']=0
for i in itseg_a2:
    ITshows[i]=np.where(ITshows[i]>0,1,0)
    ITshows['A2']=ITshows['A2']+ITshows[i]

ITshows['A3']=0
for i in itseg_a3:
    ITshows[i]=np.where(ITshows[i]>0,1,0)
    ITshows['A3']=ITshows['A3']+ITshows[i]

ITshows['A41']=0
for i in itseg_a41:
    ITshows[i]=np.where(ITshows[i]>0,1,0)
    ITshows['A41']=ITshows['A41']+ITshows[i]

ITshows['A42']=0
for i in itseg_a42:
    ITshows[i]=np.where(ITshows[i]>0,1,0)
    ITshows['A42']=ITshows['A42']+ITshows[i]
    
ITshows['A4']=np.where((ITshows['A41']>=2)|(ITshows['A42']>0),1,0)

ITshows['A5']=0
for i in itseg_a5:
    ITshows[i]=np.where(ITshows[i]>0,1,0)
    ITshows['A5']=ITshows['A5']+ITshows[i]

#ITshows['A1mins']=0
#for i in itseg_a1min:
#    ITshows['A1mins']=ITshows['A1mins']+ITshows[i]

ITshows['A2mins']=0
for i in itseg_a2min:
    ITshows['A2mins']=ITshows['A2mins']+ITshows[i]
 
ITshows['A3mins']=0
for i in itseg_a3min:
    ITshows['A3mins']=ITshows['A3mins']+ITshows[i]
    
'''   
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

ITshows['seg_a4']=0
for i in itseg_a4:
    ITshows['seg_a4']=np.where((ITshows['seg_a4']==1)|((ITshows[i]>0)),1,0)
        
'''        
#ITshows['segment']=np.where(ITshows['seg_a1']>0,"A1",np.where(ITshows['seg_a2']>0,'A2','Other'))

#ITshows=ITshows[ITshows['Totalfreq312073']>0]
#print(ITshows.groupby('segment').count())
ITshows=ITshows[['HHID_PersonID', 'A1','A2','A3','A4','A5','A2mins','A3mins','Cluster','ShiftedViewindicatorallprogram','ShiftedViewindicator320916','ShiftedViewindicator337744','ShiftedViewindicator353846']]
ITshows.columns = ['HHID_PersonID', 'A1','A2','A3','A4','A5','A2mins','A3mins','Cluster_IT','ShiftedViewindicatorallprogram','ShiftedViewindicator320916','ShiftedViewindicator337744','ShiftedViewindicator353846']
ITshows=ITshows.drop_duplicates('HHID_PersonID')

df=pd.merge(ITshows,df,on='HHID_PersonID',how='right')



'''
if bcastotindicator=='T':
    OTshow2=pd.read_csv(otshowpath2)
    OTshows=pd.merge(OTshows,OTshow2,on='HHID_PersonID',how='outer')
    OTshow2=pd.read_csv(otshowpath3)
    OTshows=pd.merge(OTshows,OTshow2,on='HHID_PersonID',how='outer')
'''

OTshows=pd.read_csv(otshowpath,dtype={'HHID':'object','PersonID':'object'})
OTshows['ot0']=1
#OTshows = OTshows[OTshows['SumofTotalmins']>=100]

#hbo_mvpd = pd.read_csv(hbomvpd,dtype={'HHID':'object','PersonID':'object'})
#hbo_mvpd = hbo_mvpd[hbo_mvpd['MVPD'].isin(['DirecTV','Dish Network'])]

#OTshows = pd.merge(OTshows,hbo_mvpd,on = 'HHID_PersonID')

OTshows=OTshows[['HHID_PersonID','ot0','Mins','Clustermom']]
OTshows=OTshows.drop_duplicates('HHID_PersonID')

df=pd.merge(OTshows,df,on='HHID_PersonID',how='outer')
df.fillna(0,inplace=True)

j=1
dfotall = pd.DataFrame()
for i in otshowpath1:
    dfot = pd.read_csv(i)
    otshows = otshownames[j]
    dfot['ot'] = 0
    for a in otshows:
        if a in dfot.columns:
            dfot['ot'] = dfot['ot'] + dfot[a]
    dfot = dfot[['HHID_PersonID','ot']]
    dfot = dfot[dfot['ot']>0]
    dfotall = dfotall.append(dfot,ignore_index = True)
    
dfotall = dfotall.groupby('HHID_PersonID').sum()
dfotall.reset_index(inplace = True)
df=pd.merge(dfotall,df,on='HHID_PersonID',how='right')
df.fillna(0,inplace=True)

#quads = quads[quads['Cluster']=='Core_Occ']
#ITshows = ITshows[ITshows['Cluster']!=7]


df['finalsegment']=np.where((df['ot0']>0)&(df['Mins']>100)&(df['MVPD'].isin(['DirecTV','Dish Network'])),'A1',
                            np.where((df['ot0']>0),'A2',
                            np.where((df['Quads']==1)&(df['Cluster']=='Core_Occ')&(df['A1']>=2),'A3',
                            np.where((df['A2']>=2)&(df['A2mins']>200)&(df['MVPD'].isin(['DirecTV','Dish Network'])),'A4',
                            np.where((df['A3']>0)&(df['A3mins']>100)&(df['MVPD'].isin(['DirecTV','Dish Network'])),'A5',
                            np.where((df['A4']>0)&(df['MVPD']!='DirecTV')&(df['MVPD']!='Dish Network')&((df['ShiftedViewindicator320916'].isin([2,3]))|(df['ShiftedViewindicator337744'].isin([2,3]))|(df['ShiftedViewindicator353846'].isin([2,3]))),'A6',
                            np.where((df['ot']>0)|(df['A5']>0),'ot','Other')))))))

print(df[['finalsegment','HHID_PersonID']].groupby('finalsegment').count())
df.to_csv("AudienceSizing_dancemom3.csv",index=False)
print(datetime.datetime.now())

#test=pd.merge(OTshows2,df,on='HHID_PersonID')
#df['test']=np.where(df['BroadcastOT']>0,1,0)
#df.groupby('test').count()
