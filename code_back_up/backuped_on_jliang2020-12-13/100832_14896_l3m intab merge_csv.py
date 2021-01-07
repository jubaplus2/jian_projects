import pandas as pd

#print 'reading files'
quads = pd.read_csv('/Users/jubaplus1/Desktop/A&E/Leah Remini Scientology and the Aftermath S2/INTAB 12.csv')

personintab1 = pd.read_csv('/Users/jubaplus1/Desktop/A&E/Leah Remini Scientology and the Aftermath S2/Sizing/personintab_160101_170611.csv')
#personintab2 = pd.read_csv('/disk2/Projects/AudienceSizing/type26_16325_16925.csv')
# personintab3 = pd.read_csv('/disk2/Projects/AudienceSizing/type26_16325_16925.csv')
# ... ... 
personintab = pd.concat([personintab1], ignore_index = True)


#print 'adding HHID_PersonID column'
quads['HHID_PersonID'] = quads['HHID'].apply(lambda x: '%07d'%(x,)) + '_'+ quads['PersonID'].apply(lambda x: '%02d'%(x,))
personintab['HHID_PersonID'] = personintab['HHID'].apply(lambda x: '%07d'%(x,)) + '_'+ personintab['PersonID'].apply(lambda x: '%02d'%(x,))

#print 'calculating PersonIntab mean'
group_p = personintab[personintab['PersonIntab'] != 0].groupby('HHID_PersonID')['PersonIntab'].mean()
group_hh = personintab[personintab['HHIntab'] != 0].groupby('HHID_PersonID')['HHIntab'].mean()
personintab_mean = pd.DataFrame({'HHID_PersonID': group_p.keys(), 'PersonIntab': group_p.values, 'HHIntab': group_hh.values})



#print 'merging csv'
result = pd.merge(quads, personintab_mean, how = 'left', on = ['HHID_PersonID'])

#print 'output to result.csv'
result.to_csv('/Users/jubaplus1/Desktop/A&E/Leah Remini Scientology and the Aftermath S2/Sizing/result.csv', index = False)

#print 'finished.'