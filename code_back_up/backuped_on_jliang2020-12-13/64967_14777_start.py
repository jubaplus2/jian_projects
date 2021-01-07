from campadset import createAutomationCampaign
from structureparser import readStructureConfig, generateconfigurationAutomation, deliveryEstimateAutomation
from search import searchString, searchIncome, searchBehaviors, searchDMA, searchRegions, targetSearch
from location import getZip, getZipDetails
import time
timestr = time.strftime("%Y%m%d-%H%M%S")


CONFIG = '../config/'
OUTPUT = '../output/'
delimiter = '\t'


def readConfigurationFiles(inputFile):
	inputdict = {}
	with  open(inputFile,'r') as f:
		t = f.read()
		t1 = t.split('\n')
		if '' in t1:
			t1.remove('')
	for elements in t1:
		if '\r' in elements:
			elements = elements.strip('\r')
		v = elements.split(':')
		inputdict[v[0]]= v[1]
	return inputdict

def getFileList(inputdict, inputstring):
	includeFiles = None
        if ',' in inputdict[inputstring]:
                includeFiles = inputdict[inputstring].split(',')
        else:
                includeFiles = [inputdict[inputstring]]
	return includeFiles


def searchAdInterestDetailsFunc(inputDict, lines):
	if inputDict[lines[0]].get('interests'):
		inputDict[lines[0]]['interests'].append(searchString(lines[2], lines[1]))
	else:
		inputDict[lines[0]]['interests']= [searchString(lines[2], lines[1])]

def searchAdWorkEmployerDetailsFunc(inputDict, lines):
	if inputDict[lines[0]].get('work_employers'):
		inputDict[lines[0]]['work_employers'].append(searchString(lines[2], lines[1]))
	else:
		inputDict[lines[0]]['work_employers']= [searchString(lines[2], lines[1])]

def searchIncomeDetailsFunc(inputDict, lines):
	if inputDict[lines[0]].get('income'):
		inputDict[lines[0]]['income'].append(searchIncome(lines[2], lines[1]))
	else:
		inputDict[lines[0]]['income']= [searchIncome(lines[2], lines[1])]

def searchBehaviorsDetailsFunc(inputDict, lines):
	if inputDict[lines[0]].get('behaviors'):
		inputDict[lines[0]]['behaviors'].append(searchBehaviors(lines[2], lines[1]))
	else:
		inputDict[lines[0]]['behaviors']= [searchBehaviors(lines[2], lines[1])]

def searchFamilyDetailsFunc(inputDict, lines):
	if inputDict[lines[0]].get('family_statuses'):
		inputDict[lines[0]]['family_statuses'].append(searchIncome(lines[2], lines[1]))
	else:
		inputDict[lines[0]]['family_statuses']= [searchIncome(lines[2], lines[1])]

def searchZipCodeFunc(inputDict, lines):
	#'geo_locations''zips']
	#zip can be a number or a filename like szip.csv
	#if Filename, read the file and get first 2500 of it max, FB supports upto 2500 for estimate cal
	if lines[2].isdigit():
		if inputDict[lines[0]].get('zips'):
			inputDict[lines[0]]['zips'].append(getZip(lines[2]))
		else:
			inputDict[lines[0]]['zips']= [getZip(lines[2])]
	else:
		#Looks to be a zip file
		zipDetailsList = getZipDetails(CONFIG+lines[2])
		if len(zipDetailsList) > 2500:
			#zipDetailsList = zipDetailsList[0:2500]
			zipDetailsList = zipDetailsList[0:2000]
		if inputDict[lines[0]].get('zips'):
                        inputDict[lines[0]]['zips'].extend(zipDetailsList)
                else:
                        inputDict[lines[0]]['zips']= zipDetailsList

def searchGeoMarketFunc(inputDict, lines):
	#"geo_locations"  "geo_markets"
	if inputDict[lines[0]].get('geo_markets'):
		inputDict[lines[0]]['geo_markets'].append(searchDMA(lines[2], lines[1]))
	else:
		inputDict[lines[0]]['geo_markets']= [searchDMA(lines[2], lines[1])]

def searchRegionsFunc(inputDict, lines):
	#"geo_locations"  "regions"
	if inputDict[lines[0]].get('regions'):
		inputDict[lines[0]]['regions'].append(searchRegions(lines[2], lines[1]))
	else:
		inputDict[lines[0]]['regions']= [searchRegions(lines[2], lines[1])]

def targetingsearchFunc(inputDict, lines):
	v = targetSearch(lines[2], lines[1])
	if v:
		targetType = v.get('type')
		if targetType:
			targetid = v['id']
			if inputDict[lines[0]].get(targetType):
				inputDict[lines[0]][targetType].append(targetid)
			else:
				inputDict[lines[0]][targetType]= [targetid]
	else:
		print 'targetingsearchFunc returned nothing'

searchdict = {'adinterest': searchAdInterestDetailsFunc,
		'adworkemployer': searchAdWorkEmployerDetailsFunc,
		'adTargetingCategory:class=income' : searchIncomeDetailsFunc,
		'adTargetingCategory:class=behaviors':  searchBehaviorsDetailsFunc,
		'adTargetingCategory:class=family_statuses' : searchFamilyDetailsFunc,
		'adgeolocation:adzipcode' : searchZipCodeFunc,
		'adgeolocation:geo_market': searchGeoMarketFunc,
		'adgeolocation:region': searchRegionsFunc,
		'targetingsearch': targetingsearchFunc,
		'targetingsearch:education_statuses': targetingsearchFunc,
                'targetingsearch:education_schools': targetingsearchFunc,
                'targetingsearch:college_years': targetingsearchFunc,
                'targetingsearch:education_majors': targetingsearchFunc,
                'targetingsearch:work_employers': targetingsearchFunc,
                'targetingsearch:work_positions': targetingsearchFunc,

	   }

def filldicts(inputfile,inputDict):
	with open(inputfile, 'r') as f:
		t = f.read()
		t1 = t.split('\n')
		if '' in t1:
			t1.remove('')
		for lines in t1:
	        	if '\r' in lines:
				lines = lines.strip('\r')
			lines = lines.split(delimiter)
			#inputDict[lines[0]] = {}
			if not inputDict.get(lines[0]):
				inputDict[lines[0]] = {}
			lines[1] = lines[1].strip()
			if searchdict.get(lines[1]):
				searchdict.get(lines[1])(inputDict, lines)

if __name__ == "__main__":
	inputdict = readConfigurationFiles(CONFIG+'inputfiles.txt')

	#read Campaign Details
	#campaignid = createAutomationCampaign(CONFIG+inputdict['campaign'])

	#read structure details
	header, printHeader, dictDetails = readStructureConfig(CONFIG+inputdict['structure'])

	#read include details
	includeFiles = getFileList(inputdict, 'include')
	includeDict = {}
	for files in includeFiles:
		filldicts(CONFIG+files, includeDict)
	print 'Debug'
	print 'IncludeDict'
	print includeDict

	#read exclude details
	excludeFiles = getFileList(inputdict, 'exclude')
	excludeDict = {}
	for files in excludeFiles:
		filldicts(CONFIG+files, excludeDict)
	print 'Debug'
	print 'ExcludeDict'
	print excludeDict

	#read location details
	locationFiles = getFileList(inputdict, 'location')
	locationDict = {}
	for files in locationFiles:
                filldicts(CONFIG+files, locationDict)
	print 'Debug'
        print 'LocationDict'
        print locationDict
	resultheader = 'AduienceCount\tEstimate_DAU\tEstimate_MAU\tMinBid\tMedianBid\tMaxBid\n'
	fileName = OUTPUT+'EstimateResult_'+timestr+'.csv'
        f = open(fileName, 'w')
        f.write(header+'\t'+resultheader)
	printHeader = [t for t in printHeader  if t!= '']
	for key in printHeader:	
		values = dictDetails[key]
		if '\r' in values[-1]:
			values[-1] = values[-1].strip('\r')
		payload= generateconfigurationAutomation(values, includeDict, excludeDict, locationDict, header)
		estresult = deliveryEstimateAutomation(payload)
		result = '\t'.join([key.split('_')[0], key, '\t'.join(values), estresult])
                f.write(result+'\n')
