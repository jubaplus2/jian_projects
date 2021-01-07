from campadset import createAutomationCampaign, createAdset, getcampaignID
from structureparser import readStructureConfigSummary, generateconfigurationAutomationNonTV, deliveryEstimateAutomation
from search import searchString, searchIncome, searchBehaviors, searchDMA, searchRegions, targetSearch
from location import getZip, getZipDetails
from estimatecal import getsofarListDetails, writeToList, config
import time
import json
import itertools
timestr = time.strftime("%Y%m%d-%H%M%S")


CONFIG = '../config/'
OUTPUT = '../output/'
delimiter = '\t'

def getFinalCombinatlist(permdictKeys, conditiondictKeys, locationKeys):
        permList = []
        for x in range(1,len(permdictKeys)+1):
                permList.extend(list(itertools.combinations(permdictKeys, x)))
        #Permutations based on all conditions are met like condition_1, condition_2, condition_3 all together
        conditionList = []
        for elements,value in conditiondictKeys.iteritems():
                conditionList.append(list(value))


        finaList = [locationKeys, permList]
        finaList.extend(conditionList)
        combList = list(itertools.product(*finaList))
        print len(combList)
        return combList


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

def filldictsCond(inputfile,inputDict, condfiledict):
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
                        if condfiledict.get(inputfile):
                                condfiledict[inputfile] =  condfiledict[inputfile]|set([lines[0]])
                        else:
                                condfiledict[inputfile] = set([lines[0]])
                        if not inputDict.get(lines[0]):
                                inputDict[lines[0]] = {}
                        lines[1] = lines[1].strip()
                        if searchdict.get(lines[1]):
                                searchdict.get(lines[1])(inputDict, lines)

if __name__ == "__main__":
	inputdict = readConfigurationFiles(CONFIG+'inputfilesnontv.txt')

	#read Campaign Details
	campaignid = getcampaignID()
	if len(campaignid)  == 0:
		campaignid = createAutomationCampaign(CONFIG+inputdict['campaign'])
		fl1 = open('campaignid.txt','w')
                fl1.write(campaignid)
                fl1.close()
	else:
		print 'Campaign id %s is present in campaignid.txt so all adsets are created under same campaign' %campaignid
	#read structure details
	header, printHeader, dictDetails = readStructureConfigSummary(CONFIG+inputdict['summary'])

	#read permutation details
        permFiles = getFileList(inputdict, 'permutation')
        permDict = {}
        for files in permFiles:
                filldicts(CONFIG+files, permDict)
        print 'Debug'
        print 'PermutationDict'
        print permDict

	#read condition details
        conditionFiles = getFileList(inputdict, 'condition')
        conditionDict = {}
        conditionFilesDict = {}
        for files in conditionFiles:
                filldictsCond(CONFIG+files, conditionDict, conditionFilesDict)
        print 'Debug'
        print 'ConditionDict'
        print conditionDict

	#read location details
	locationFiles = getFileList(inputdict, 'location')
	locationDict = {}
	for files in locationFiles:
                filldicts(CONFIG+files, locationDict)
	print 'Debug'
        print 'LocationDict'
        print locationDict
	permdictKeys = permDict.keys()
        conditiondictKeys = conditionDict.keys()
        locationKeys = locationDict.keys()
	combinationList = getFinalCombinatlist(permdictKeys, conditionFilesDict, locationKeys)
	lsofarList = getsofarListDetails()
        newAdCreation = 0
	fileName = OUTPUT+'AdsetDetails_'+timestr+'.txt'
	fadset = open(fileName, 'w')
	fadset.write('AdsetID\tAdsetName\n')

	for key, values in dictDetails.iteritems():
		if '\r' in values[-1]:
                        values[-1] = values[-1].strip('\r')
		for elements in combinationList:
			payload= generateconfigurationAutomationNonTV(values, elements, permDict, conditionDict, locationDict, header)
			if json.dumps(payload['targeting_spec']) not in lsofarList:
				print 'adsetCreation'
				payload['targeting'] = payload['targeting_spec']
				t = [elements[0], '_'.join(elements[1]), '_'.join(elements[2:])]
				#payload['name'] = key
				adname = '_'.join(t)
				payload['name'] = adname
				payload['billing_event'] = 'IMPRESSIONS'
				payload['access_token'] = config['access_token']
				if values[4] == 'Lifetime Budget':
					payload['is_autobid'] = 'TRUE'
					#remember the format is $100.00
					payload['lifetime_budget'] = values[5].strip().split('$')[-1].replace('.','')
					payload['status']= 'PAUSED'
					payload['start_time'] =  values[6]+ ' '+'EST'
					payload['end_time'] =  values[7]+ ' '+'EST'
				del  payload['targeting_spec']
				adsetid = createAdset(payload, campaignid)
				if adsetid != 0:
					#print 'AdsetName=%s' %key
					print 'AdsetName=%s' %adname
					writeToList(payload)
					#fadset.write(adsetid+'\t'+key+'\n')
					fadset.write(adsetid+'\t'+adname+'\n')
				newAdCreation = 1
			else:
				print 'Adset not created as entry is there in listsofar.txt'
				if newAdCreation == 0:
					print 'All Ad creation completed'