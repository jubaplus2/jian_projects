## -*- coding: utf-8 -*-
"""
Created on Mon Jun  6 15:02:11 2016

@author: Jubaplus 
"""
import datetime
print(datetime.datetime.now())

#####update the direcotry--all output files will be saved in this directory
foldername='/Users/jubaplus1/Desktop/WETV/Tamar and Vince/Sizing'
personintab='/Users/jubaplus1/Desktop/WETV/Tamar and Vince/Sizing/personintab_160101_170319.csv'
demofilepath='/Users/jubaplus1/Desktop/WETV/Tamar and Vince/Sizing/Demographicpersonlevel_160101_170319.csv'
quadspath='/Users/jubaplus1/Desktop/WETV/Tamar and Vince/Sizing/t&v_s3s4.csv'
viewershippath=''
itshowpath='/Users/jubaplus1/Desktop/WETV/Tamar and Vince/TamarVince_showaffinity_code.csv'
#GUHH
otshowpath='/Users/jubaplus1/Desktop/WETV/Tamar and Vince/Sizing/GUHH_l12m_312.csv'
bcastotindicator='F'
otshowpath2=''
otshowpath4=''

####put startdate and enddate for the project here:
Startdate=20160319
Enddate=20170319

#A1 in quadspath (quads)
#A2 in itseg_a1 and otshowpath (guhh viewers), ANY SHOW
#A3 in itseg_a2, ANY SHOW
#A4 in itseg_a3, ANY SHOW

####it show information: leave it blank if no show falls into the category
###only work for 2 to 4 
numberofitshows=3
itseg_a1=['Totalfreq385186','Totalfreq343256','Totalfreq353099','Totalfreq375479']
itseg_a2=['Totalfreq415600','Totalfreq410052','Totalfreq408558','Totalfreq402099','Totalfreq402100']
itseg_a3=['Totalfreq312073','Totalfreq320916']
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

personintab=intab[['HHID','PersonID','PersonIntab']]
personintab=personintab.groupby(['HHID','PersonID']).mean()
personintab.reset_index(inplace=True)
hhintab=intab[['HHID','HHIntab']]
hhintab=hhintab.drop_duplicates()
hhintab=hhintab.groupby('HHID').mean()
hhintab.reset_index(inplace=True)
intab=pd.merge(personintab,hhintab,on='HHID')

demo=pd.read_csv(demofilepath,dtype={'HHID':'object','PersonID':'object'})
quads=pd.read_csv(quadspath,dtype={'HHID':'object','PersonID':'object'})

df=pd.merge(quads,demo,on=['HHID','PersonID'],how='right')
df['Quads'].fillna(0,inplace=True)
df=pd.merge(df,intab,on=['HHID','PersonID'],how='outer')

#viewership=pd.read_csv(viewershippath,dtype={'HHID':'object','PersonID':'object'})
#df=pd.merge(df,viewership,on=['HHID','PersonID'],how='outer')

df['HHID_PersonID']=df['HHID']+'_'+df['PersonID']

#viewership['HHID_PersonID']=viewership['HHID']+'_'+viewership['PersonID']
ITshows=pd.read_csv(itshowpath)
#ITshows=pd.merge(ITshows,viewership,on='HHID_PersonID',how='left')

ITshows['A2']=0
for i in itseg_a1:
    ITshows['A2']=ITshows['A2']+ITshows[i]
    
ITshows['A3']=0
for i in itseg_a2:
    ITshows['A3']=ITshows['A3']+ITshows[i]

ITshows['A4']=0
for i in itseg_a3:
    ITshows[i]=np.where(ITshows[i]>0,1,0)
    ITshows['A4']=ITshows['A4']+ITshows[i]




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
ITshows=ITshows[['HHID_PersonID', 'A2', 'A3','A4','ShiftedViewindicatorallprogram']]
ITshows=ITshows.drop_duplicates('HHID_PersonID')

df=pd.merge(ITshows,df,on='HHID_PersonID',how='outer')



'''
if bcastotindicator=='T':
    OTshow2=pd.read_csv(otshowpath2)
    OTshows=pd.merge(OTshows,OTshow2,on='HHID_PersonID',how='outer')
    OTshow2=pd.read_csv(otshowpath3)
    OTshows=pd.merge(OTshows,OTshow2,on='HHID_PersonID',how='outer')
'''

OTshows=pd.read_csv(otshowpath,dtype={'HHID':'object','PersonID':'object'})
OTshows['guhh']=1
OTshows['HHID_PersonID']=OTshows['HHID']+'_'+OTshows['PersonID']


OTshows=OTshows[['HHID_PersonID','guhh']]
OTshows=OTshows.drop_duplicates('HHID_PersonID')

df=pd.merge(OTshows,df,on='HHID_PersonID',how='right')
df.fillna(0,inplace=True)

df['finalsegment']=np.where((df['Quads']>0),'A1',
                            np.where((df['A2']>0)|(df['guhh']>0),'A2',
                                     np.where(df['A3']>0,'A3',
                                              np.where(df['A4']==2,'A4','Other'))))

df.to_csv("new_AudienceSizing_TandV.csv",index=False)
print(datetime.datetime.now())

#test=pd.merge(OTshows2,df,on='HHID_PersonID')
#df['test']=np.where(df['BroadcastOT']>0,1,0)
#df.groupby('test').count()
