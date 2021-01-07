## -*- coding: utf-8 -*-
"""
Created on Mon Jun  6 15:02:11 2016

@author: Jubaplus 
"""
import datetime
print(datetime.datetime.now())


#####update the direcotry--all output files will be saved in this directory
foldername='/Users/jubaplus1/Desktop/A&E/Criminal Movies/Robert Durst/Sizing'
personintab='/Users/jubaplus1/Desktop/A&E/Criminal Movies/Robert Durst/Sizing/personintab_150101_170108.csv'
demofilepath='/Users/jubaplus1/Desktop/A&E/Criminal Movies/Robert Durst/Sizing/Demographicpersonlevel_150101_170101.csv'
quadspath=''
viewershippath=''
hbomvpd=''
itshowpath='/Users/jubaplus1/Desktop/A&E/Criminal Movies/Robert Durst/Sizing/code_robert_showaffinitynew_cluster.csv'
otshowpath='/Users/jubaplus1/Desktop/A&E/Criminal Movies/Robert Durst/OT/Broadcast/broadcast_programfreq.csv'
bcastotindicator='T'
otshowpath1 = '/Users/jubaplus1/Desktop/A&E/Criminal Movies/Robert Durst/OT/Cable/Cable/Robert_Cable_programfreq.csv'
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
Startdate=20150101
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
#quads=pd.read_csv(quadspath,dtype={'HHID':'object','PersonID':'object'})

#df=pd.merge(quads,demo,on=['HHID','PersonID'],how='right')
#df['Quads'].fillna(0,inplace=True)
df = demo.copy()
df=pd.merge(df,intab,on=['HHID','PersonID'],how='left')

#viewership=pd.read_csv(viewershippath,dtype={'HHID':'object','PersonID':'object'})
#df=pd.merge(df,viewership,on=['HHID','PersonID'],how='outer')

df['HHID_PersonID']=df['HHID']+'_'+df['PersonID']

dfga1 = pd.read_csv(ga1)
dfga1 = dfga1[dfga1['Totalmins']>=15]
dfga1 = dfga1[['HHID_PersonID']].drop_duplicates()
dfga1['ga1'] = 1

dfga2 = pd.read_csv(ga2)
dfga2 = dfga2[dfga2['Totalmins']>=30]
dfga2 = dfga2[['HHID_PersonID']].drop_duplicates()
dfga2['ga2'] = 1

dfga3 = pd.read_csv(ga3,dtype={'HHID':'object','PersonID':'object'})
dfga3['HHID_PersonID']=dfga3['HHID']+'_'+dfga3['PersonID']
dfga3 = dfga3[dfga3['Totalmins']>=50]
dfga3 = dfga3[['HHID_PersonID']].drop_duplicates()
dfga3['ga3'] = 1

dfga4 = pd.read_csv(ga4,dtype={'HHID':'object','PersonID':'object'})
dfga4['HHID_PersonID']=dfga4['HHID']+'_'+dfga4['PersonID']
dfga4 = dfga4[dfga4['Totalmins']>=50]
dfga4 = dfga4[['HHID_PersonID']].drop_duplicates()
dfga4['ga4'] = 1


dfga5 = pd.read_csv(ga5_1,dtype={'HHID':'object','PersonID':'object'})
dfga5['HHID_PersonID']=dfga5['HHID']+'_'+dfga5['PersonID']

dfga5 = dfga5[['HHID_PersonID','Totalmins']]
dfga52 = pd.read_csv(ga5_2,dtype={'HHID':'object','PersonID':'object'})
dfga52['HHID_PersonID']=dfga52['HHID']+'_'+dfga52['PersonID']

dfga52 = dfga52[['HHID_PersonID','Totalmins']]
dfga5 = dfga5.append(dfga52,ignore_index = True)
dfga5 = dfga5.groupby('HHID_PersonID').sum()
dfga5 = dfga5[dfga5['Totalmins']>=100]
dfga5.reset_index(inplace = True)
dfga5 = dfga5[['HHID_PersonID']].drop_duplicates()
dfga5['ga5'] = 1

dfga6 = pd.read_csv(ga6_1,dtype={'HHID':'object','PersonID':'object'})
dfga6['HHID_PersonID']=dfga6['HHID']+'_'+dfga6['PersonID']

dfga6 = dfga6[['HHID_PersonID','Totalmins']]
dfga6.columns = ['HHID_PersonID','Totalmins_ga61']
dfga6['ga6_1']=1
dfga62 = pd.read_csv(ga6_2,dtype={'HHID':'object','PersonID':'object'})
dfga62['HHID_PersonID']=dfga62['HHID']+'_'+dfga62['PersonID']

dfga62 = dfga62[['HHID_PersonID','Totalmins']]
dfga62.columns = ['HHID_PersonID','Totalmins_ga62']
dfga62['ga6_2']=1
dfga6 = pd.merge(dfga6,dfga62,on = 'HHID_PersonID',how = 'outer')
dfga6.fillna(0,inplace = True)


dfga7 = pd.read_csv(ga7,dtype={'HHID':'object','PersonID':'object'})
dfga7['HHID_PersonID']=dfga7['HHID']+'_'+dfga7['PersonID']
dfga7 = dfga7[dfga7['Totalmins']>=100]
dfga7 = dfga7[['HHID_PersonID']].drop_duplicates()
dfga7['ga7'] = 1

dfga8 = pd.read_csv(ga8,dtype={'HHID':'object','PersonID':'object'})
dfga8 = dfga8[dfga8['Totalmins']>=400]
dfga8 = dfga8[['HHID_PersonID']].drop_duplicates()
dfga8['ga8'] = 1


dfgc = pd.read_csv(gc1,dtype={'HHID':'object','PersonID':'object'})
dfgc = dfgc[dfgc['Cluster']=='Core_Occ']
dfgc = dfgc[['HHID_PersonID']].drop_duplicates()
dfgc['gc'] = 1

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

df=pd.merge(dfga1,df,on='HHID_PersonID',how='right')
df=pd.merge(dfga2,df,on='HHID_PersonID',how='right')
df=pd.merge(dfga3,df,on='HHID_PersonID',how='right')
df=pd.merge(dfga4,df,on='HHID_PersonID',how='right')
df=pd.merge(dfga5,df,on='HHID_PersonID',how='right')


df=pd.merge(ITshows,df,on='HHID_PersonID',how='right')
df=pd.merge(dfga7,df,on='HHID_PersonID',how='right')
df=pd.merge(dfga8,df,on='HHID_PersonID',how='right')
df=pd.merge(dfgc,df,on='HHID_PersonID',how='right')


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
df.to_csv("AudienceSizing_robert15_1.csv",index=False)
print(datetime.datetime.now())

#test=pd.merge(OTshows2,df,on='HHID_PersonID')
#df['test']=np.where(df['BroadcastOT']>0,1,0)
#df.groupby('test').count()
