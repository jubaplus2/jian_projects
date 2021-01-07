## -*- coding: utf-8 -*-
"""
Created on Mon Jun  6 15:02:11 2016

@author: Jubaplus 
"""
import datetime
print(datetime.datetime.now())
import pandas as pd

#####update the direcotry--all output files will be saved in this directory
foldername='/Users/jubaplus1/Desktop/Sundance/Top of Lake/Sizing'
personintab='/Users/jubaplus1/Desktop/A&E/Dance Mom/Sizing/personintab_160101_170611.csv'
demofilepath='/Users/jubaplus1/Desktop/A&E/Dance Mom/Sizing/Demographicpersonlevel_160101_170611.csv'
filea2='/Users/jubaplus1/Desktop/Sundance/Top of Lake/Sizing/sund_l3m_0611_100mins.csv'
filea4='/Users/jubaplus1/Desktop/Sundance/Top of Lake/Sizing/sund_l12m_0611_100mins.csv'
quadspath=''
viewershippath=''
itshowpath='/Users/jubaplus1/Desktop/Sundance/Top of Lake/TOTL_showaffnity_cluster_code.csv'

otshwopathmins=['/Users/jubaplus1/Desktop/Sundance/Top of Lake/OT/Cable/programmin.csv'
,'/Users/jubaplus1/Desktop/Sundance/Top of Lake/OT/Sundance/programmin.csv']
bcastotindicator = 'T'
otshowpathfreq = ['/Users/jubaplus1/Desktop/Sundance/Top of Lake/OT/Cable/programfreq.csv'
,'/Users/jubaplus1/Desktop/Sundance/Top of Lake/OT/Sundance/programfreq.csv']

####put startdate and enddate for the project here:
Startdate=20160611
Enddate=20170611
gender = ['F','M']
minage = 25
maxage = 54

#a1 = file:otshowpathfreq, otshwopathmins and itshowpath. shows in itseg_a1 and a1otshows, any shows, mins>100
#a2 = filea2
#a3 = file: itshowpath. shows in itseg_a3, any shows, mins>100
#a4 = file: itshowpath. shows in itseg_a4, any shows, mins>100
#a5 = file: itshowpath. shows in itseg_a5, any shows, mins>100
#ot = file: otshowpathfreq and otshwopathmins. shows in otshownames1, any shows, mins>100

#7.20
#a1 = file:otshowpathfreq, otshwopathmins and itshowpath. shows in itseg_a1 and a1otshows, any shows, mins>100
#new a2 = filea2, w25-54
#new a3 = rest of filea2
#new a4 = filea4, only w25-54 and not in filea2
#new a5 (combine old a4 and a5) = file: itshowpath. shows in itseg_a5, any shows, mins>100
#a6 = file: itshowpath. shows in itseg_a6, any shows, mins>100
#ot = file: otshowpathfreq and otshwopathmins. shows in otshownames1, any shows, mins>100

####it show information: leave it blank if no show falls into the category
###only work for 2 to 4 
numberofitshows=3
itseg_a1=['Totalfreq364702']
itseg_a1min=['Totalmins364702']
itseg_a5=['Totalfreq385635','Totalfreq363743','Totalfreq362201','Totalfreq406382']
itseg_a5min=['Totalmins385635','Totalmins363743','Totalmins362201','Totalmins406382']
itseg_a6=['Totalfreq407461','Totalfreq420313']
itseg_a6min=['Totalmins407461','Totalmins420313']
###otshownames:
#otshowpath1
otshownames1=['AMC  _TALKING DEAD             ','FX   _TABOO                    ','WGNA _OUTSIDERS                ','HBOM _GIRLS                    ','ENT  _ARRANGEMENT              ','FX   _FEUD: BETTE AND JOAN     ','LIF  _PROJECT RUNWAY JUNIOR    ','HBOM _SILICON VALLEY           ','STZP _BLACK SAILS 4            ','BRVO _LADIES OF LONDON         ','POP  _DAWSONS CREEK            ','FXX  _YOURE THE WORST          ','FX   _LOUIE                    ','POP  _DAWSONS CREEK            ','FX   _AMERICANS                ','FS2  _WORLD CUP QUALIFIER L    ','BRVO _GIRLFRND GUIDE TO DIVORCE']

otshownames = {1:otshownames1
               }
a1otshows=['SUND _GOMORRAH                 ','SUND _ALL IN THE FAMILY        ']

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

filea2shows=pd.read_csv(filea2)
filea2shows['A2'] = 1
filea2shows = filea2shows[['HHID_PersonID','A2']].drop_duplicates()
df=pd.merge(filea2shows,df,on='HHID_PersonID',how='right')

filea4shows=pd.read_csv(filea4)
filea4shows['A4'] = 1
filea4shows = filea4shows[['HHID_PersonID','A4']].drop_duplicates()
df=pd.merge(filea4shows,df,on='HHID_PersonID',how='right')

#viewership['HHID_PersonID']=viewership['HHID']+'_'+viewership['PersonID']
ITshows=pd.read_csv(itshowpath)
#ITshows=pd.merge(ITshows,viewership,on='HHID_PersonID',how='left')
ITshows['A1mins']=0
for i in itseg_a1min:
    ITshows['A1mins']=ITshows['A1mins']+ITshows[i]

ITshows['A5mins']=0
for i in itseg_a5min:
    ITshows['A5mins']=ITshows['A5mins']+ITshows[i]
 
ITshows['A6mins']=0
for i in itseg_a6min:
    ITshows['A6mins']=ITshows['A6mins']+ITshows[i]
    
ITshows=ITshows[['HHID_PersonID','A1mins', 'A5mins','A6mins','Cluster','ShiftedViewindicatorallprogram']]
ITshows.columns = ['HHID_PersonID', 'A1mins','A5mins','A6mins','Cluster_IT','ShiftedViewindicatorallprogram']
ITshows=ITshows.drop_duplicates('HHID_PersonID')

df=pd.merge(ITshows,df,on='HHID_PersonID',how='right')


dfotall = pd.DataFrame()
for i in otshwopathmins:
    dfot = pd.read_csv(i)
    dfot['ot'] = 0
    dfot['a1ot'] = 0
    for a in otshownames1:
        if a in dfot.columns:
            dfot['ot'] = dfot['ot'] + dfot[a]
    for a in a1otshows:
        if a in dfot.columns:
            dfot['a1ot'] = dfot['a1ot'] + dfot[a]
    dfot = dfot[['HHID_PersonID','ot','a1ot']]
    dfot = dfot[(dfot['ot']>0)|(dfot['a1ot']>0)]
    dfotall = dfotall.append(dfot,ignore_index = True)
    
dfotall = dfotall.groupby('HHID_PersonID').sum()
dfotall.reset_index(inplace = True)

df=pd.merge(dfotall,df,on='HHID_PersonID',how='right')
df.fillna(0,inplace=True)

#quads = quads[quads['Cluster']=='Core_Occ']
#ITshows = ITshows[ITshows['Cluster']!=7]
df['A1mins'] = df['A1mins'] + df['a1ot']

df['finalsegment']=np.where((df['A1mins']>100),'A1',
                            np.where((df['A2']>0)&(df['Gender'] == 'F'),'A2',
                            np.where((df['A2']>0),'A3',
                            np.where((df['A4']>0)&(df['Gender'] == 'F'),'A4',
                            np.where((df['A5mins']>100),'A5',
                            np.where((df['A6mins']>100),'A6',
                            np.where((df['ot']>100),'ot','Other')))))))

print(df[['finalsegment','HHID_PersonID']].groupby('finalsegment').count())
df.to_csv("AudienceSizing_totl720.csv",index=False)
print(datetime.datetime.now())

#test=pd.merge(OTshows2,df,on='HHID_PersonID')
#df['test']=np.where(df['BroadcastOT']>0,1,0)
#df.groupby('test').count()
