## -*- coding: utf-8 -*-

"""

Created on Mon Jun  6 15:02:11 2016



@author: Jubaplus 

"""

import datetime

print(datetime.datetime.now())





#####update the direcotry--all output files will be saved in this directory

foldername='/Users/jubaplus1/Desktop/TV1/Behind the Movement/Sizing'

personintab='/Users/jubaplus1/Desktop/TV1/Behind the Movement/Sizing/personintab_160101_171217.csv'

demofilepath='/Users/jubaplus1/Desktop/TV1/Uncensored/Sizing/Demographicpersonlevel_160101_171203.csv'

quadspath=''

viewershippath=''

itshowpath='/Users/jubaplus1/Desktop/TV1/Behind the Movement/TV1_BTM_showaffnity.csv'

otshowpath='/Users/jubaplus1/Desktop/TV1/Behind the Movement/OT/TV1/programmin.csv'

bcastotindicator='T'

otshowpath1 = ['/Users/jubaplus1/Desktop/TV1/Behind the Movement/OT/Cable/programmin.csv']

ga1='/Users/jubaplus1/Desktop/TV1/Behind the Movement/Sizing/goodtime_L12M.csv'

ga3='/Users/jubaplus1/Desktop/TV1/Behind the Movement/Sizing/have_greenleaf_queen_iyanla_L12M.csv'

ga5='/Users/jubaplus1/Desktop/TV1/Uncensored/Sizing/TV1_l12m_1206.csv'



####put startdate and enddate for the project here:

Startdate=20160101

Enddate=20171217

 

gender = ['F','M']

minage = 25

maxage = 54



#a1 file is ga1, mvpd='Dish Network' or 'DirecTV' and mins >100

#a2 file is ga1, and mins >100

#a3 file is ga3, mvpd='Dish Network' or 'DirecTV' and mins >100

#a4 file is ga3, and mins >100

#a5 file is ga5, and mins >100

#ot file is otshowpath and otshowpath1, show is otshownames1-2


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

otshownames1=['TV1  _COSBY SHOW, THE          ','TV1  _DIFFERENT WORLD          ','TV1  _SANFORD & SON            ','TV1  _LIVING SINGLE            ','TV1  _JEFFERSONS               ']

#otshowpath2

otshownames2=['BET  _MARTIN                   ','GSN  _FAMILY FEUD (HARVEY)     ','BET  _MEET THE BROWN           ','OWN  _DATELINE ON OWN          ','MTV2 _WAYANS BROS.             ','VH1  _LOVE & HIP HOP ATLANTA 6 ','VH1  _BASKETBALL WIVES 6       ','DSNY _K.C. UNDERCOVER          ','BRVO _REAL HOUSEWIVES ATLANTA  ']

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





dfga1 = pd.read_csv(ga1,dtype={'HHID':'object','PersonID':'object'})

dfga1['HHID'] = dfga1['HHID'].apply(lambda x:x.zfill(7))

dfga1['PersonID'] = dfga1['PersonID'].apply(lambda x:x.zfill(2))

dfga1['HHID_PersonID']=dfga1['HHID']+'_'+dfga1['PersonID']

dfga1 = dfga1[dfga1['Totalmins']>=100]

dfga1 = dfga1[['HHID_PersonID']].drop_duplicates()

dfga1['ga1'] = 1



dfga3 = pd.read_csv(ga3,dtype={'HHID':'object','PersonID':'object'})

dfga3['HHID'] = dfga3['HHID'].apply(lambda x:x.zfill(7))

dfga3['PersonID'] = dfga3['PersonID'].apply(lambda x:x.zfill(2))

dfga3['HHID_PersonID']=dfga3['HHID']+'_'+dfga3['PersonID']

dfga3 = dfga3[dfga3['Totalmins']>=100]

dfga3 = dfga3[['HHID_PersonID']].drop_duplicates()

dfga3['ga3'] = 1





dfga5 = pd.read_csv(ga5,dtype={'HHID':'object','PersonID':'object'})

dfga5['HHID'] = dfga5['HHID'].apply(lambda x:x.zfill(7))

dfga5['PersonID'] = dfga5['PersonID'].apply(lambda x:x.zfill(2))

dfga5['HHID_PersonID']=dfga5['HHID']+'_'+dfga5['PersonID']

dfga5 = dfga5[dfga5['Totalmins']>=100]

dfga5 = dfga5[['HHID_PersonID']].drop_duplicates()

dfga5['ga5'] = 1



 

df=pd.merge(dfga1,df,on='HHID_PersonID',how='right')

df=pd.merge(dfga3,df,on='HHID_PersonID',how='right')

df=pd.merge(dfga5,df,on='HHID_PersonID',how='right')



otshownames = otshownames1 + otshownames2

ot1 = pd.read_csv(otshowpath)

ot1['ot1'] = 0

for a in otshownames:

    if a in ot1.columns:

        ot1['ot1'] = ot1['ot1'] + ot1[a]

ot1 = ot1[['HHID_PersonID','ot1']]

ot1 = ot1[ot1['ot1']>0]

ot1 = ot1.drop_duplicates('HHID_PersonID')



ot2 = pd.DataFrame()

for i in otshowpath1:

    dfot = pd.read_csv(i)

    dfot['ot2'] = 0

    for a in otshownames:

        if a in dfot.columns:

            dfot['ot2'] = dfot['ot2'] + dfot[a]

    dfot = dfot[['HHID_PersonID','ot2']]

    dfot = dfot[dfot['ot2']>0]

    ot2 = ot2.append(dfot,ignore_index = True)

ot2 = ot2.drop_duplicates('HHID_PersonID')

ot = pd.merge(ot1,ot2,on = 'HHID_PersonID',how = 'outer' )

ot['ot'] = 1

ot = ot[['HHID_PersonID','ot']]

ot = ot.drop_duplicates('HHID_PersonID')

 

      



df=pd.merge(ot,df,on='HHID_PersonID',how='right')

df.fillna(0,inplace=True)



#quads = quads[quads['Cluster']=='Core_Occ']

#ITshows = ITshows[ITshows['Cluster']!=7]





df['finalsegment']=np.where((df['ga1']==1)&(df['MVPD'].isin(['DirecTV'])),'A1',
                            np.where((df['ga1']==1),'A2',
                            np.where((df['ga3']==1)&(df['MVPD'].isin(['DirecTV'])),'A3',
                            np.where((df['ga3']==1),'A4',
                            np.where((df['ga5']==1),'A5',
                            np.where((df['ot']>0),'ot','Other'))))))



print(df[['finalsegment','HHID_PersonID']].groupby('finalsegment').count())

df.to_csv("AudienceSizing_BTM_100.csv",index=False)

print(datetime.datetime.now())



#test=pd.merge(OTshows2,df,on='HHID_PersonID')

#df['test']=np.where(df['BroadcastOT']>0,1,0)

#df.groupby('test').count()
