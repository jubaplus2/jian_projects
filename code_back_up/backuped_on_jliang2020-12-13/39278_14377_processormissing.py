import os

outputheader = 'ShowID\tNetwork\tShowName\tUSNG_100\tWeek\tDecile'
path = "/disk2/Projects/Rentrak/newData/20150921-20151004/"
outputpath = 'output/'
missingList = ['1925577','2089348', '2150061', '2194142', '2211469', '2285806', '2293230','2366514','2375987','2385204']

def readNetworkAndShoWName(inputFile):
	resultdict = {}
	with open(inputFile, 'r') as f:
		g = f.read()
		t = g.split('\n')
		t.remove('')
		for i in range(1,len(t)):
			v = t[i].split('\t')
			resultdict[v[1]] = [v[0], v[2]]
	return resultdict

def getinfoFromFileName(inputString):
	if 'reissue' in inputString:
		v = inputString.split('_')[-2].split('-')
		g = v[:-1]
		week = ''.join(g[0:3])+'-'+''.join(g[3:])
		showid = v[-1]
		return week, showid
	v = inputString.split('_')[-1]
	v1 = v.split('-')
	week = ''.join(v1[0:3])+'-'+''.join(v1[3:6])
	showid = v1[-1].split('.txt')[0]
	return week,showid

def getAllfiles(path):
	#for file in os.listdir("/disk2/Projects/Rentrak/newData/20160808-20160821"):
	fileList = []
	for file in os.listdir(path):
		if file.endswith(".txt"):
			fileList.append(file)
	return fileList

def readInputFiles(inputFile):
	with open(inputFile, 'r') as f:
		g = f.read()
		content = g.split('\r\n')
		content.remove('')
		#Discard the first element being header
		return content[1:]
		

if __name__ == "__main__": 
	outputheader = 'ShowID\tNetwork\tShowName\tUSNG_100\tWeek\tDecile'
	idNetworkShowNameDict = readNetworkAndShoWName('NetworkShownames.csv')
	fileList = getAllfiles(path)
	for files in fileList:
		week,showid = getinfoFromFileName(files)
		if  showid in missingList and idNetworkShowNameDict.get(showid):
			f = open(outputpath+'Show'+showid+'.csv', 'w')
			content = readInputFiles(path+files)
			f.write(outputheader+'\n')
			gg= showid+'\t'+'\t'.join(idNetworkShowNameDict[showid])
			for elements in content:
				lresult = gg
				vv = elements.split(',')
				lresult+='\t'+vv[0]+'\t'+week+'\t'+vv[1]
				f.write(lresult+'\n')
			f.close()
		else:
			print showid
	print 'arnab'
