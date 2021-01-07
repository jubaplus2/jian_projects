from os import listdir
import pandas as pd
import math

path = '/disk2/Projects/Rentrak/20150907-20150920/'
llpath = '/disk2/Projects/Rentrak/zip4_lat_lon_usng.csv'


def getusngzip4dict(llpath):
	usngzip4dict = {}
	with open(llpath) as f:
		next(f)
		for line in f:
			v = line.split('|')
			usngzip4dict[v[-1].replace('\r\n','')] = (elements for elements in v[:-1])
	return usngzip4dict



def getShowidToFileNameAndrefFile():
	showidToFileName = {}
	refFile = None
	result = listdir(path)
	for elements in result:
		if 'txt' in elements:
			v = elements.split('-')[-1].split('.txt')[0]
			showidToFileName[v]  = elements
		elif 'xlsx' in elements:
			refFile = elements
	return showidToFileName, refFile


def getShowidToName (refFile):
	v = pd.read_excel(path+refFile, sheetname='Sheet1')
	showidToName = {}
	c = 0
	while c<len(v):
		#if isinstance(v['series_no'][c], str)
		if not math.isnan(v['series_no'][c]):
			showidToName[str(int(v['series_no'][c]))] = str(v['title'][c])
		c+=1
	return showidToName
	
	
if __name__ == '__main__':
	tt = getusngzip4dict(llpath)
	showidToFileName, refFile = getShowidToFileNameAndrefFile()
	showidToName = getShowidToName(refFile)
	print showidToName
	
