import pandas as pd

gr1 = [1] *5
#gr1 = [0, 0, 0, 0, 1]



showidName = {}
showidName [2375987] = 'Jacksons: Next Generation'
showidName [1887065] = 'Real Housewives Atlanta'
showidName [2293230] = 'Love & Hip Hop Hollywood'



def filldict(inputfile):
	v = pd.read_csv(inputfile,sep = '\t')
	x = {}
	for index, row in v.iterrows():
		#key = ':'.join([str(elements) for elements in [v.iloc[index,3], v.iloc[index,4], v.iloc[index,5]]])
		#Usng,zip ,zip4
		key = ':'.join([str(elements) for elements in [row[3], row[4], row[5]]])
		if not x.get(key):
			x[key] = [row[0], row[6] , row[7],  row[8],  row[9],  row[10]]
		else:
			print 'arnab'
	return x

def printresult(key, value):
	tt = key.split(':')
        #showid, showname, usng, zip, zip4
	if showidName.get(value[0]):
		vv = [str(x) for x in [value[0] , showidName[value[0]], tt[0], tt[1], tt[2], value[1], value[2], value[3], value[4], value[5]]]
		result = '\t'.join(vv)
		print result
		return result

def checkGr4(inputlist):
	if '0' not in inputlist:
		return 0
	else:
		return 1

def checkGroup4(inputdictlist):
	#Rank  not 0 in any of the weeks and should have non zero rank across all the week
	keyset = set(inputdictlist[0].keys())
	lendict = len(inputdictlist)
	complist = [0] * lendict
	for i in  xrange(1,lendict):
		keyset &= set(inputdictlist[i].keys())
	for keys in list(keyset):
		t = [checkGr4(inputdict[keys][1:]) for inputdict in inputdictlist]
		if cmp(t, complist) == 0:
			for inputdict in inputdictlist:
				printresult(keys, inputdict[keys])
				inputdict.pop(keys)


def checkGroup3(inputdictlist):
	#Rank 1 all the weeks for any show should have non zero rank across all the week
	keyset = set(inputdictlist[0].keys())
	lendict = len(inputdictlist)
	complist = [0] * lendict
	for i in  xrange(1,lendict):
		keyset &= set(inputdictlist[i].keys())
	keysTobConsidered = []
	for keys in list(keyset):
		for inputdict in inputdictlist:
			if cmp(inputdict[keys][1:], gr1) == 0:
				keysTobConsidered.append(keys)

	for elements in keysTobConsidered:
		for inputdict in inputdictlist:
			if  0 not in inputdict.get(elements)[1:]:
				printresult(elements, inputdict[elements])
				inputdict.pop(elements)

def checkGr2(inputlist):
	if '1' in inputlist and '0' not in inputlist:
		return 0
	else:
		return 1

def checkGroup2(inputdictlist):
	#Rank 1 in any of the weeks and should have non zero rank across all the week
	keyset = set(inputdictlist[0].keys())
	lendict = len(inputdictlist)
	complist = [0] * lendict
	for i in  xrange(1,lendict):
		keyset &= set(inputdictlist[i].keys())
	for keys in list(keyset):
		t = [checkGr2(inputdict[keys][1:]) for inputdict in inputdictlist]
		if cmp(t, complist) == 0:
			for inputdict in inputdictlist:
				printresult(keys, inputdict[keys])
				inputdict.pop(keys)


def checkGroup1(inputdictlist):
	#Rank 1 across all weeks for all shows
	keyset = set(inputdictlist[0].keys())
	lendict = len(inputdictlist)
	complist = [0] * lendict
	for i in  xrange(1,lendict):
		keyset &= set(inputdictlist[i].keys())
	for keys in list(keyset):
		t = [cmp(inputdict[keys][1:],gr1) for inputdict in inputdictlist]
		if cmp(t, complist) == 0:
			for inputdict in inputdictlist:
				printresult(keys, inputdict[keys])
				inputdict.pop(keys)
dict2375987 = filldict('/disk2/arnab/rentrak/week4/2375987/Show2375987W4.csv')
dict2293230 = filldict('/disk2/arnab/rentrak/week4/2293230/Show2293230W4.csv')
#dict1887065 = filldict('/disk2/arnab/rentrak/week4/1887065/Show1887065W4.csv')
'''
dict2375987 = filldict('tt')
dict2293230 = filldict('tt1')
#dict1887065 = filldict('tt2')
'''

print "For Group1"
#checkGroup1([dict2375987, dict2293230])
checkGroup1([dict2293230])
'''
print "For Group2"
checkGroup2([dict2375987, dict2293230])
print "For Group3"
checkGroup3([dict2375987])
print "For Group4"
#checkGroup4([dict2375987, dict2293230])
'''
