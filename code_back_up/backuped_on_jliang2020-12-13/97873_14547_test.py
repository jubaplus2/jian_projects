import pandas as pd
import gc
v = pd.read_csv('sample.tsv','\t', dtype = str)
#v = v.loc[0:].astype('str')
xx = list(v.iloc[0])
print xx
tt =  pd.DataFrame()
yy = [xx, xx]
#t = pd.DataFrame(yy,  dtype = str)
#v.append(pd.DataFrame(yy,  dtype = str))
x = {}
lzip = zip(['ShowID', 'Network_no', 'ShowName', 'USNG_100', 'Zip', 'Zip4', 'Rank20150907-20150920', 'Rank20150921-20151004'], xx)
for elements in lzip:
	x[elements[0]] = elements[1]

abc = pd.DataFrame([xx])
abc.to_csv('rerere.csv', '\t', index = False, mode = 'a', header=False)
abc.to_csv('rerere.csv', '\t', index = False, mode = 'a', header=False)
#abc = pd.DataFrame([xx,xx])
#abc.to_csv('rerere.csv', '\t', index = False, mode = 'a', header=False)
print v
v.to_csv('rerere.csv', '\t', index =False, mode = 'a', header=False)
