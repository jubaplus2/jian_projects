## -*- coding: utf-8 -*-
"""
Created on Mon Jun  6 15:02:11 2016

@author: Jubaplus 
"""
import datetime
print(datetime.datetime.now())


#####update the direcotry--all output files will be saved in this directory
foldername='/Users/jubaplus1/Desktop/A&E/Criminal Movies/Robert Durst/Sizing'
personintab='/Users/jubaplus1/Desktop/A&E/Criminal Movies/Robert Durst/Sizing/personintab_160101_170806.csv'
demofilepath='/Users/jubaplus1/Desktop/A&E/Criminal Movies/Robert Durst/Sizing/Demographicpersonlevel_160101_170813.csv'
quadspath=''
viewershippath=''
hbomvpd=''
itshowpath='/Users/jubaplus1/Desktop/A&E/Criminal Movies/Robert Durst/Sizing/code_robert_showaffinitynew_cluster.csv'
otshowpath='/Users/jubaplus1/Desktop/A&E/Criminal Movies/Robert Durst/OT/Broadcast/programfreq.csv'
bcastotindicator='T'
otshowpath1 = '/Users/jubaplus1/Desktop/A&E/Criminal Movies/Robert Durst/OT/Cable/Cable_programfreq.csv'
ga1='/Users/jubaplus1/Desktop/A&E/Criminal Movies/Robert Durst/Sizing/group_a1.csv'
ga2='/Users/jubaplus1/Desktop/A&E/Criminal Movies/Robert Durst/Sizing/group_a2.csv'
ga3='/Users/jubaplus1/Desktop/A&E/Criminal Movies/Robert Durst/Sizing/group_a3.csv'
ga4='/Users/jubaplus1/Desktop/A&E/Criminal Movies/Robert Durst/Sizing/group_a4.csv'
ga5_1='/Users/jubaplus1/Desktop/A&E/Criminal Movies/Robert Durst/Sizing/group_a4.csv'
ga5_2='/Users/jubaplus1/Desktop/A&E/Criminal Movies/Robert Durst/Sizing/lif_intervention_l12m.csv'
ga6_1='/Users/jubaplus1/Desktop/A&E/Criminal Movies/Robert Durst/Sizing/aen_intervention_l12m.csv'
ga6_2='/Users/jubaplus1/Desktop/A&E/Criminal Movies/Robert Durst/Sizing/lmn_intervention_l12m.csv'
ga7='/Users/jubaplus1/Desktop/A&E/Criminal Movies/Robert Durst/Sizing/group_a7.csv'
ga8='/Users/jubaplus1/Desktop/A&E/Criminal Movies/Robert Durst/Sizing/lif_l3m_movie.csv'
gc1='/Users/jubaplus1/Desktop/A&E/Criminal Movies/Robert Durst/Sizing/lif_l3m.csv'
####put startdate and enddate for the project here:
Startdate=20160101
Enddate=20170813
 
gender = ['F']
minage = 25
maxage = 54

#a1 file is ga1 and mvpd='Dish Network' or 'DirecTV' and mins >15
#a2 file is ga2 and mvpd='Dish Network' or 'DirecTV' and mins >30
#a3 file is ga3 and mvpd='Dish Network' or 'DirecTV' and mins >50
#a4 file is ga4 and mvpd='Dish Network' or 'DirecTV' and mins >50
#a5 file is ga5_1 and ga5_2. any one of the two shows and total mins >100 
#a6 file is ga6_1 and ga6_2 and itshowpath. two of four shows are in itseg_a1. any shows and total mins >100 
#a7 file is ga7 and mins >100
#a8 file is ga8 and mins >400
#c1 file is gc1 and Cluster='Core_Occ'
#ot file is otshowpath and otshowpath1, show is otshownames1 and otshownames2

####it show information: leave it blank if no show falls into the category
###only work for 2 to 4 
numberofitshows=2
itseg_a1=['Totalfreq389185','Totalfreq383595']
itseg_a1min=['Totalmins389185','Totalmins383595']
#otshowpath=hbo
#hbomvpd


###otshownames:
#otshowpath1
otshownames1=['NBC  _LAW AND ORDER:SVU        ','NBC  _CHICAGO PD               ','CBS  _CSI: CYBER               ','NBC  _MYSTERIES OF LAURA       ','NBC  _GRIMM                    ','CBS  _CRIMETIME SATURDAY       ','ABC  _SECRETS AND LIES         ','NBC  _AQUARIUS                 ']
#otshowpath2
otshownames2=['HLN  _FORENSIC FILES           ','OWN  _DATELINE ON OWN          ','SPIKE_JAIL                     ','TLC  _DATELINE:  REAL LIFE MYST','ID   _SCORNED: LOVE KILLS      ','TNT  _GRIMM                    ','SPIKE_COPS (O)                 ','ID   _PERFECT MURDER, THE      ','OXYG _SNAPPED: KILLER COUPLES  ','TNT  _COLD JUSTICE             ','TLC  _DATELINE ON TLC          ']
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

ITshows['A3']=0
for i in itseg_a2:
    ITshows[i]=np.where(ITshows[i]>0,1,0)
    ITshows['A3']=ITshows['A3']+ITshows[i]

ITshows['A5']=0
for i in itseg_a3:
    ITshows[i]=np.where(ITshows[i]>0,1,0)
    ITshows['A5']=ITshows['A5']+ITshows[i]


ITshows['A1mins']=0
for i in itseg_a1min:
    ITshows['A1mins']=ITshows['A1mins']+ITshows[i]

ITshows['A5mins']=0
for i in itseg_a3min:
    ITshows['A5mins']=ITshows['A5mins']+ITshows[i]
    
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
ITshows=ITshows[['HHID_PersonID', 'A1','A3','A5','A1mins','A5mins','Cluster','ShiftedViewindicatorallprogram']]
ITshows.columns = ['HHID_PersonID', 'A1','A3','A5','A1mins','A5mins','Cluster_IT','ShiftedViewindicatorallprogram']
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
OTshows = OTshows[OTshows['SumofTotalmins']>=100]

hbo_mvpd = pd.read_csv(hbomvpd,dtype={'HHID':'object','PersonID':'object'})
hbo_mvpd = hbo_mvpd[hbo_mvpd['MVPD'].isin(['DirecTV','Dish Network'])]

OTshows = pd.merge(OTshows,hbo_mvpd,on = 'HHID_PersonID')

OTshows['hbo']=1
OTshows=OTshows[['HHID_PersonID','hbo']]
OTshows=OTshows.drop_duplicates('HHID_PersonID')

df=pd.merge(OTshows,df,on='HHID_PersonID',how='outer')
df.fillna(0,inplace=True)

j=0
dfotall = pd.DataFrame()
for i in otshowpath1:
    j = j+1
    dfot = pd.read_csv(i)
    otshows = otshownames[j]
    dfot['ot'] = 0
    for a in otshows:
        dfot['ot'] = dfot['ot'] + dfot[a]
    dfot = dfot[['HHID_PersonID','ot']]
    dfotall = dfotall.append(dfot,ignore_index = True)
    
dfotall = dfotall.drop_duplicates(['HHID_PersonID'])
df=pd.merge(dfotall,df,on='HHID_PersonID',how='right')
df.fillna(0,inplace=True)

#quads = quads[quads['Cluster']=='Core_Occ']
#ITshows = ITshows[ITshows['Cluster']!=7]


df['finalsegment']=np.where((df['A1']>0)&(df['A1mins']>100)&(df['MVPD'].isin(['DirecTV','Dish Network'])),'A1',
                            np.where((df['A1']>0)&(df['Cluster_IT']!=7),'A2',
                            np.where((df['A3']>0),'A3',
                            np.where((df['Quads']==1)&(df['Cluster']=='Core_Occ'),'A4',
                            np.where((df['A5']>0)&(df['A5mins']>100)&(df['MVPD'].isin(['DirecTV','Dish Network'])),'A5',
                                     np.where(df['ot']>0,'A6','Other'))))))

print(df[['finalsegment','HHID_PersonID']].groupby('finalsegment').count())
df.to_csv("AudienceSizing_robert.csv",index=False)
print(datetime.datetime.now())

#test=pd.merge(OTshows2,df,on='HHID_PersonID')
#df['test']=np.where(df['BroadcastOT']>0,1,0)
#df.groupby('test').count()
