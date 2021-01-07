# -*- coding: utf-8 -*-
"""
Created on Wed Jun 15 17:52:25 2016

@author: Jubaplus
"""

import pandas as pd
import numpy as np

df=pd.read_csv("/Users/jubaplus1/Downloads/AEN_Robertdaypartsnew.csv",dtype={'Episode Name':'object'},sep='\t')
df.columns

test=df[['Program Code','Program Name','Episode Name']]
test=test.drop_duplicates()
test.to_csv('/Users/jubaplus1/Desktop/Epnames.csv',index=False)

test=pd.read_csv('/Users/jubaplus1/Epnames.csv')
df=pd.merge(df,test,on=['Program Code','Program Name','Episode Name'])
df['Program Name']=df['NewProgramName']
df['Program Code']=df['NewProgramCode']

#########live+3#############
#df=df[df['Minimum Play Delay Minutes']<=4500]

totalfre=df[['Cable Network ID', 'Program Code', 'Program Name','Telecast Number','HHID_PersonID']]
totalfre=totalfre.drop_duplicates()
totalfre=totalfre.groupby(['HHID_PersonID','Program Code', 'Program Name']).count()
totalfre.reset_index(inplace=True)  
totalfre=totalfre[['HHID_PersonID', 'Program Code', 'Program Name', 'Telecast Number']]
totalfre.columns=['HHID_PersonID', 'Program Code', 'Program Name', 'Totalfreq']

df['mins']=df['End Minute of Program Viewing']-df['Start Minute of Program Viewing']
totalmins=df[['Program Code', 'Program Name','mins','HHID_PersonID']]
totalmins=totalmins.groupby(['HHID_PersonID','Program Code', 'Program Name']).sum()
totalmins.reset_index(inplace=True)  
totalmins.columns=['HHID_PersonID', 'Program Code', 'Program Name', 'Totalmins']


hhintab=df[['Person In-tab Weight','HHID_PersonID']]
hhintab=hhintab.groupby('HHID_PersonID').mean()
hhintab.reset_index(inplace=True)  

shiftview=df[['Program Code', 'Program Name','HHID_PersonID','Time Shifted Viewing Code']]
shiftview=shiftview.drop_duplicates()
shiftview['Time Shifted Viewing Code']=np.where(shiftview['Time Shifted Viewing Code']==1,1,0)
shiftview=shiftview.groupby(['HHID_PersonID','Program Code', 'Program Name']).mean()
shiftview.reset_index(inplace=True)  
shiftview['Time Shifted Viewing Code']=np.where(shiftview['Time Shifted Viewing Code']==1,1,
np.where(shiftview['Time Shifted Viewing Code']==0,2,3))
shiftview.columns=['HHID_PersonID', 'Program Code', 'Program Name', 'ShiftedViewindicator']

shiftview2=df[['HHID_PersonID','Time Shifted Viewing Code']]
shiftview2=shiftview2.drop_duplicates()
shiftview2['Time Shifted Viewing Code']=np.where(shiftview2['Time Shifted Viewing Code']==1,1,0)
shiftview2=shiftview2.groupby(['HHID_PersonID']).mean()
shiftview2.reset_index(inplace=True)  
shiftview2['Time Shifted Viewing Code']=np.where(shiftview2['Time Shifted Viewing Code']==1,1,
np.where(shiftview2['Time Shifted Viewing Code']==0,2,3))
shiftview2.columns=['HHID_PersonID','ShiftedViewindicatorallprogram']
hhintab=pd.merge(hhintab,shiftview2,on='HHID_PersonID',how='outer')

allfile=pd.merge(totalfre,totalmins,on=['HHID_PersonID', 'Program Code', 'Program Name'])
allfile=pd.merge(allfile,shiftview,on=['HHID_PersonID', 'Program Code', 'Program Name'])

df2=pd.DataFrame()
program=df[['Program Code','Program Name']]
program=program.drop_duplicates()
for i in program['Program Code']:
    df2=allfile[allfile['Program Code']==i]
    df2.columns=['HHID_PersonID', 'Program Code'+str(i), 'Program Name'+str(i), 'Totalfreq'+str(i),
       'Totalmins'+str(i), 'ShiftedViewindicator'+str(i)]
    hhintab=pd.merge(hhintab,df2,on='HHID_PersonID',how='outer')
hhintab=hhintab.fillna(0)

for i,j in zip(program['Program Code'],program['Program Name']):
    hhintab['Program Code'+str(i)]=i
    hhintab['Program Name'+str(i)]=j

hhintab.to_csv("/Users/jubaplus1/Desktop/personlevel.csv",index=False)






