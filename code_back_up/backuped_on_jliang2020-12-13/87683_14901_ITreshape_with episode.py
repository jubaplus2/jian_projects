# -*- coding: utf-8 -*-
"""
Created on Wed Jun 15 17:52:25 2016

@author: Jubaplus
"""

import pandas as pd
import numpy as np

df=pd.read_csv("/Users/jubaplus1/Downloads/IT-HH-ViewershipDayPartsNew.csv",sep="\t",dtype={'Episode Name':'object'})
df.columns
df=df.fillna('na')
'''
test=df[['Program Code','Program Name','Episode Name','Broadcast Date']]
test=test.drop_duplicates()
test.to_csv('/Users/jubaplus1/Downloads/EpnamesMTV.csv',index=False)
'''
test=pd.read_csv('/Users/jubaplus1/Downloads/EpnamesMTV.csv')
df=pd.merge(df,test,on=['Program Code','Program Name','Episode Name','Broadcast Date'])
df['Program Name']=df['NewProgramName']
df['Program Code']=df['NewProgramCode']

#########live+3#############
df=df[df['Minimum Play Delay Minutes']<=4500]

totalfre=df[['Cable Network ID', 'Program Code', 'Program Name','Telecast Number','Household Number']]
totalfre=totalfre.drop_duplicates()
totalfre=totalfre.groupby(['Household Number','Program Code', 'Program Name']).count()
totalfre.reset_index(inplace=True)  
totalfre=totalfre[['Household Number', 'Program Code', 'Program Name', 'Telecast Number']]
totalfre.columns=['Household Number', 'Program Code', 'Program Name', 'Totalfreq']

df['mins']=df['End Minute of Program Viewing']-df['Start Minute of Program Viewing']
totalmins=df[['Program Code', 'Program Name','mins','Household Number']]
totalmins=totalmins.groupby(['Household Number','Program Code', 'Program Name']).sum()
totalmins.reset_index(inplace=True)  
totalmins.columns=['Household Number', 'Program Code', 'Program Name', 'Totalmins']


hhintab=df[['HH In-tab Weight','Household Number']]
hhintab=hhintab.groupby('Household Number').mean()
hhintab.reset_index(inplace=True)  

shiftview=df[['Program Code', 'Program Name','Household Number','Time Shifted Viewing Code']]
shiftview=shiftview.drop_duplicates()
shiftview['Time Shifted Viewing Code']=np.where(shiftview['Time Shifted Viewing Code']==1,1,0)
shiftview=shiftview.groupby(['Household Number','Program Code', 'Program Name']).mean()
shiftview.reset_index(inplace=True)  
shiftview['Time Shifted Viewing Code']=np.where(shiftview['Time Shifted Viewing Code']==1,1,
np.where(shiftview['Time Shifted Viewing Code']==0,2,3))
shiftview.columns=['Household Number', 'Program Code', 'Program Name', 'ShiftedViewindicator']

shiftview2=df[['Household Number','Time Shifted Viewing Code']]
shiftview2=shiftview2.drop_duplicates()
shiftview2['Time Shifted Viewing Code']=np.where(shiftview2['Time Shifted Viewing Code']==1,1,0)
shiftview2=shiftview2.groupby(['Household Number']).mean()
shiftview2.reset_index(inplace=True)  
shiftview2['Time Shifted Viewing Code']=np.where(shiftview2['Time Shifted Viewing Code']==1,1,
np.where(shiftview2['Time Shifted Viewing Code']==0,2,3))
shiftview2.columns=['Household Number','ShiftedViewindicatorallprogram']
hhintab=pd.merge(hhintab,shiftview2,on='Household Number',how='outer')

allfile=pd.merge(totalfre,totalmins,on=['Household Number', 'Program Code', 'Program Name'])
allfile=pd.merge(allfile,shiftview,on=['Household Number', 'Program Code', 'Program Name'])

df2=pd.DataFrame()
program=df[['Program Code','Program Name']]
program=program.drop_duplicates()
for i in program['Program Code']:
    df2=allfile[allfile['Program Code']==i]
    df2.columns=['Household Number', 'Program Code'+str(i), 'Program Name'+str(i), 'Totalfreq'+str(i),
       'Totalmins'+str(i), 'ShiftedViewindicator'+str(i)]
    hhintab=pd.merge(hhintab,df2,on='Household Number',how='outer')
hhintab=hhintab.fillna(0)

for i,j in zip(program['Program Code'],program['Program Name']):
    hhintab['Program Code'+str(i)]=i
hhintab['Program Name'+str(i)]=j

hhintab.to_csv("/Users/jubaplus1/Downloads/showaffinityMTV.csv",index=False)






