import json
import itertools
from readzip import getZipDetails

def readfile(inputfile):
	v = None
	with open(inputfile) as f:
		t = f.read()
		v = json.loads(t)
	return v


mainConf = readfile('paramconfig.json')

familydetails = readfile('familyconfig.json')

excludefamilydetails = readfile('excludefamilyconfig.json')

sZip = getZipDetails('szip.csv')

pZip = getZipDetails('pzip.csv')

retailerDetails = readfile('retailersparamconfig.json')

excluderetailerDetails = readfile('excluderetailersparamconfig.json')

prod1 = readfile('prodparamconfig1.json')
prod2 = readfile('prodparamconfig2.json')
prod3 = readfile('prodparamconfig3.json')
prod4 = readfile('prodparamconfig4.json')
prod5 = readfile('prodparamconfig5.json')
prod6 = readfile('prodparamconfig6.json')

incomedetails = readfile('incomeconfig.json')
excludeincomedetails = readfile('excludeincomeconfig.json')

configDict = {}

configDict['Pzip1'] = pZip
configDict['Szip1'] = sZip
zipList = ['Pzip1', 'Szip1']

configDict['FamilyWChildren'] = familydetails
configDict['FamilyWoChildren'] = excludefamilydetails
familyList = ['FamilyWChildren', 'FamilyWoChildren']

configDict['Retailer'] = retailerDetails
configDict['ExcludeRetailer'] = excluderetailerDetails
retailerList = ['Retailer', 'ExcludeRetailer']

configDict['Furniture'] = prod1
configDict['Mattress'] = prod2
configDict['Garden'] = prod3
configDict['Kitchen'] = prod4
configDict['Bath'] = prod5
configDict['Home'] = prod6
productList = ['Furniture', 'Mattress', 'Garden', 'Kitchen', 'Bath', 'Home']

configDict['25k-75k'] = incomedetails
configDict['Exclude25k-75k'] = excludeincomedetails
incomeList = ['25k-75k', 'Exclude25k-75k']
#print configDict

#print 'arnab'

allList = [zipList, productList, familyList, retailerList, incomeList]
combList = list(itertools.product(*allList))
#print combList 
print len(combList)
