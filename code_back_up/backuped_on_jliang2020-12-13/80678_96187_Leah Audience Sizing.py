## -*- coding: utf-8 -*-
"""
Created on Mon Jun  6 15:02:11 2016

@author: Jubaplus 
"""
import datetime
print(datetime.datetime.now())

#####update the direcotry--all output files will be saved in this directory
foldername='/Users/jubaplus1/Desktop/A&E/Leah Remini Scientology and the Aftermath S2/Sizing'
personintab='/Users/jubaplus1/Desktop/A&E/Leah Remini Scientology and the Aftermath S2/Sizing/personintab_160101_170611.csv'
demofilepath='/Users/jubaplus1/Desktop/A&E/Leah Remini Scientology and the Aftermath S2/Sizing/Demographicpersonlevel_160101_170611.csv'
quadspath='/Users/jubaplus1/Desktop/A&E/Leah Remini Scientology and the Aftermath S2/Sizing/aen_l3m_0611.csv'
viewershippath=''
#hbo mvpd
hbomvpd='/Users/jubaplus1/Desktop/A&E/Leah Remini Scientology and the Aftermath S2/Mapped.csv'
itshowpath='/Users/jubaplus1/Desktop/A&E/Leah Remini Scientology and the Aftermath S2/Leah_IT_showaffnity_code.csv'
otshowpath='/Users/jubaplus1/Desktop/A&E/Leah Remini Scientology and the Aftermath S2/AEN_LeahS2_HBO_showaffnityv2.csv'
bcastotindicator='T'
otshowpath1 = ['/Users/jubaplus1/Desktop/A&E/Leah Remini Scientology and the Aftermath S2/OT/Cable/Cable_programfreq.csv'
,'/Users/jubaplus1/Desktop/A&E/Leah Remini Scientology and the Aftermath S2/OT/AEN/AEN_programfreq.csv'
,'/Users/jubaplus1/Desktop/A&E/Leah Remini Scientology and the Aftermath S2/OT/FYI/FYI_programfreq.csv'
,'/Users/jubaplus1/Desktop/A&E/Leah Remini Scientology and the Aftermath S2/OT/HISTORY/HIS_programfreq.csv'
,'/Users/jubaplus1/Desktop/A&E/Leah Remini Scientology and the Aftermath S2/OT/LIF/LIF_programfreq.csv'
,'/Users/jubaplus1/Desktop/A&E/Leah Remini Scientology and the Aftermath S2/OT/LMN/LMN_programfreq.csv']

####put startdate and enddate for the project here:
Startdate=20160613
Enddate=20170611
###hbo 20140929

gender = ['F','M']
minage = 25
maxage = 54

#a1 file is itshowpath; show is itseg_a1; any one of the two shows; and mvpd=dish or dtv; and mins of the two shows>100 
#a2 file is itshowpath; show is itseg_a1; any one of the two shows, ### Cluster_IT not equal 7 ###
#a3 file is itshowpath; show is itseg_a2; any one of the two shows
#a4 file is quadspath, ### Cluster = Core_Occ ###
#a5 file is itshowpath; show is itseg_a3; plus hbo, hbo file is otshowpath, hbo demo file is hbomvpd; any one of the three shows; and mvpd=dish or dtv; and mins of the three shows >100 
#ot file is otshowpath1-6, show is otshownames1-6


####it show information: leave it blank if no show falls into the category
###only work for 2 to 4 
numberofitshows=3
itseg_a1=['Totalfreq416294','Totalfreq417593']
itseg_a1min=['Totalmins416294','Totalmins417593']
itseg_a2=['Totalfreq363743','Totalfreq394532']
itseg_a3=['Totalfreq288526','Totalfreq366428'] 
itseg_a3min=['Totalmins288526','Totalmins366428'] 
#otshowpath=hbo
#hbomvpd


###otshownames:
#otshowpath1
otshownames1=['FRFM _MIDDLE, THE              ','USA  _CSI                      ','TLC  _HOARDING: BURIED ALIVE   ','SUND _LAW & ORDER              ','FRFM _LAST MAN STANDING        ']
#otshowpath2
otshownames2=['AEN  _THE FIRST 48             ','AEN  _DOG THE BOUNTY HUNTER    ']
#otshowpath3
otshownames3=['FYI  _KITCHEN NIGHTMARES       ','FYI  _WIFE SWAP                ']
#otshowpath4
otshownames4=['HIST _PAWN STARS               ','HIST _CURSE OF OAK ISLAND      ']
#otshowpath5
otshownames5=['LIF  _GREYS ANATOMY            ','LIF  _LITTLE WOMEN ATL         ']
#otshowpath6
otshownames6=['LMN  _MY CRAZY EX              ']

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
df.to_csv("AudienceSizing_leah77.csv",index=False)
print(datetime.datetime.now())

#test=pd.merge(OTshows2,df,on='HHID_PersonID')
#df['test']=np.where(df['BroadcastOT']>0,1,0)
#df.groupby('test').count()
