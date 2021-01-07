# -*- coding: utf-8 -*-
"""
Created on Tue Dec 13 11:24:25 2016

@author: Jubaplus
"""
import pandas as pd
programlist=pd.read_csv('/home/nielsen/MicroSegment/Engine/Client/AMC/FeartheWalkingDeadv2/programlist.csv')
unifiedintab=pd.read_csv('/home/nielsen/MicroSegment/Engine/Client/AMC/FeartheWalkingDeadv2/personintab.csv')
outputpath='/home/nielsen/MicroSegment/Engine/Client/AMC/FeartheWalkingDeadv2/'

programlist1=programlist[['programfinal']]
programlist1=programlist1.drop_duplicates()
programlist1=programlist1['programfinal']
programlist2=programlist1
programlist3=programlist1
programlist4=programlist1
programlist5=programlist1


####3 programs
plist=list()
for i in programlist1:
    for j in programlist2:
        if (i!=j):
            a=[i,j]
            a.sort()
            var=a[0]+'|'+a[1]
            plist.append(var)

dflist=pd.DataFrame(plist,columns=['plist'])
dflist2=dflist.drop_duplicates()
print dflist2

plist=list()
for i in programlist1:
    for j in programlist2:
        for x in programlist3:
            if (i!=j)&(i!=x)&(x!=j):
                 a=[i,j,x]
                 a.sort()
                 var=a[0]+'|'+a[1]+'|'+a[2]
                 plist.append(var)

dflist=pd.DataFrame(plist,columns=['plist'])
dflist3=dflist.drop_duplicates()

plist=list()
for i in programlist1:
    for j in programlist2:
        for x in programlist3:
            for y in programlist4:
                if (i!=j)&(i!=x)&(x!=j)&(i!=y)&(y!=j)&(y!=x):
                    a=[i,j,x,y]
                    a.sort()
                    var=a[0]+'|'+a[1]+'|'+a[2]+'|'+a[3]
                    plist.append(var)

dflist=pd.DataFrame(plist,columns=['plist'])
dflist4=dflist.drop_duplicates()


###2programs
dfjall=pd.DataFrame()
for i in programlist1:
    dfj=programlist[programlist['programfinal']==i]
    dfj=dfj[['programfinal','HHID_PersonID']]
    dfj.columns=['programfinal1','HHID_PersonID']
    dfj=pd.merge(dfj,programlist[['HHID_PersonID','programfinal']],on=['HHID_PersonID'])
    dfj=dfj[dfj['programfinal']!=dfj['programfinal1']]
    dfj=pd.merge(dfj,unifiedintab,on=['HHID_PersonID'])
    dfj=dfj[['programfinal1','programfinal','PersonIntab']]
    dfj=dfj.groupby(['programfinal1','programfinal']).sum()
    dfj.reset_index(inplace=True)
    dfjall=dfjall.append(dfj,ignore_index=True)

dfjall['plist']=dfjall['programfinal1']+'|'+dfjall['programfinal']
dfjall=pd.merge(dfjall,dflist2,on='plist')
dfjall=dfjall.sort_values('PersonIntab',ascending=0)
dfjall=dfjall.reset_index(drop=True)
dfjall=dfjall.head(500)

dfjall.to_csv(outputpath+'programcore2.csv',index=False)


###3programs
dflist=dflist2['plist']
dfjall=pd.DataFrame()
for i in dflist:
    a=i.split('|')[0]
    b=i.split('|')[1]
    dfi=programlist[programlist['programfinal']==a]
    dfi=dfi[['programfinal','HHID_PersonID']]
    dfi.columns=['programfinal1','HHID_PersonID']
    dfj=programlist[programlist['programfinal']==b]
    dfj=dfj[['programfinal','HHID_PersonID']]
    dfj.columns=['programfinal2','HHID_PersonID']
    dfj=pd.merge(dfj,dfi,on=['HHID_PersonID'])
    dfj=pd.merge(dfj,programlist[['HHID_PersonID','programfinal']],on=['HHID_PersonID'])
    dfj=dfj[dfj['programfinal']!=dfj['programfinal2']]
    dfj=dfj[dfj['programfinal']!=dfj['programfinal1']]
    dfj=pd.merge(dfj,unifiedintab,on=['HHID_PersonID'])
    dfj=dfj[['programfinal1','programfinal2','programfinal','PersonIntab']]
    dfj=dfj.groupby(['programfinal1','programfinal2','programfinal']).sum()
    dfj.reset_index(inplace=True)
    dfjall=dfjall.append(dfj,ignore_index=True)

dfjall['plist']=dfjall['programfinal1']+'|'+dfjall['programfinal2']+'|'+dfjall['programfinal']
dfjall=pd.merge(dfjall,dflist3,on='plist')
dfjall=dfjall.sort_values('PersonIntab',ascending=0)
dfjall=dfjall.reset_index(drop=True)
dfjall=dfjall.head(500)

dfjall.to_csv(outputpath+'programcore3.csv',index=False)

####4 programs

dflist=dflist3['plist']
dfjall=pd.DataFrame()

for i in dflist:
    a=i.split('|')[0]
    b=i.split('|')[1]
    c=i.split('|')[2]
    dfi=programlist[programlist['programfinal']==a]
    dfi=dfi[['programfinal','HHID_PersonID']]
    dfi.columns=['programfinal1','HHID_PersonID']
    dfj=programlist[programlist['programfinal']==b]
    dfj=dfj[['programfinal','HHID_PersonID']]
    dfj.columns=['programfinal2','HHID_PersonID']
    dfj=pd.merge(dfj,dfi,on=['HHID_PersonID'])
    dfx=programlist[programlist['programfinal']==c]
    dfx=dfx[['programfinal','HHID_PersonID']]
    dfx.columns=['programfinal3','HHID_PersonID']
    dfj=pd.merge(dfj,dfx,on=['HHID_PersonID'])
    dfj=pd.merge(dfj,programlist[['HHID_PersonID','programfinal']],on=['HHID_PersonID'])
    dfj=dfj[dfj['programfinal']!=dfj['programfinal2']]
    dfj=dfj[dfj['programfinal']!=dfj['programfinal1']]
    dfj=dfj[dfj['programfinal']!=dfj['programfinal3']]
    dfj=pd.merge(dfj,unifiedintab,on=['HHID_PersonID'])
    dfj=dfj[['programfinal1','programfinal2','programfinal3','programfinal','PersonIntab']]
    dfj=dfj.groupby(['programfinal1','programfinal2','programfinal3','programfinal']).sum()
    dfj.reset_index(inplace=True)
    dfjall=dfjall.append(dfj,ignore_index=True)

dfjall['plist']=dfjall['programfinal1']+'|'+dfjall['programfinal2']+'|'+dfjall['programfinal']+'|'+dfjall['programfinal3']
dfjall=pd.merge(dfjall,dflist4,on='plist')
dfjall=dfjall.sort_values('PersonIntab',ascending=0)
dfjall=dfjall.reset_index(drop=True)
dfjall=dfjall.head(500)
dfjall.to_csv(outputpath+'programcore4v2.csv',index=False)
