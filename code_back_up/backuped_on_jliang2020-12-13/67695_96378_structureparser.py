import json
import copy
from estimatecal import getDeliverEstimate, getReachEstimate, config
dmaList = []
topITList = []
sundanceList = []
topOTList = []
excludeOTTList = []

objectiveMappings = {'Video views': 'VIDEO_VIEWS'}
objectiveMappings = {'Store visits': 'STORE_VISITS'}

def readStructureConfig(inputFile):
	f = open(inputFile, 'r')	
	t = f.read()
	t1 = t.split('\n')
	if '' in t1:
		t1.remove('')
	header = t1[0]
	printHeader = []
	dictDetails = {}
	for elements in t1[1:]:
		values = elements.split('\t')
		printHeader.append(values[1])
		dictDetails[values[1]] = values[2:]
	f.close()
	if '' in dictDetails:
		del dictDetails['']
	return header, printHeader, dictDetails

def readStructureConfigSummary(inputFile):
	f = open(inputFile, 'r')	
	t = f.read()
	t1 = t.split('\n')
	if '' in t1:
		t1.remove('')
	header = t1[0]
	printHeader = []
	dictDetails = {}
	for idx, elements in enumerate(t1[1:]):
		values = elements.split('\t')
		printHeader.append(str(idx)+'_'+values[0])
		dictDetails[str(idx)+'_'+values[0]] = values[0:]
	f.close()
	if '' in dictDetails:
		del dictDetails['']
	return header, printHeader, dictDetails

def readConfigFile(inputFile):
        with open(inputFile, 'r') as f:
                t = f.read()
                configdict = json.loads(t)
        return configdict

def readDMADetails(inputFile):
	global dmaList
	t = open(inputFile).read()
	t1 = t.split('\n')
	if '' in t1:
		t1.remove('')
	for elements in t1[2:]:
		dmaList.append(json.loads(elements))

def readAdInterestDetails(inputFile):
	interestDetails = []
        t = open(inputFile).read()
        t1 = t.split('\n')
        if '' in t1:
                t1.remove('')
        for elements in t1[1:]:
                interestDetails.append(json.loads(elements))
	return interestDetails

def getIndexes(header , inputString):
	t = header.split('\t')
	indexList = []
	for i, j in enumerate(t):
		if inputString in j:
			indexList.append(i)
	return indexList

def getIndexesExcludeIndexName(header , inputString):
	vv = getIndexes(header, inputString)
	#-2 is as per Structure input file first col is index and 2nd col is name hence excluding that
	vv = [elements -2 for elements in vv]
	return vv

def getIndexesExcludeIndexNameStep4(header , inputString):
        vv = getIndexes(header, inputString)
        return vv

def processIncludes(includeDict, value, targetingSpec):	
	if includeDict.get(value):
		includeDetails = includeDict.get(value)
		for k,v in includeDetails.iteritems():
			if targetingSpec.get('flexible_spec'):
				#targetingSpec['flexible_spec'].append({k:v})
				#All include elements are to be OR so should be part of the same dict inside list
                                targetingSpec['flexible_spec'][0][k] = v
			else:
				targetingSpec['flexible_spec'] = [{k:v}]

def processIncludesNonTV(includeDict, value, targetingSpec):	
	x = {}
	if includeDict.get(value):
		includeDetails = includeDict.get(value)
		for k,v in includeDetails.iteritems():
			if x.get('flexible_spec'):
				v1 = x.get('flexible_spec')
				for elements in v1:
					if elements.get(k):
						elements.append(v)
					else:
						elements[k] = v
			else:
				x['flexible_spec'] = [{k:v}]
	if targetingSpec.get('flexible_spec'):
		if x.get('flexible_spec'):
			targetingSpec['flexible_spec'].extend(copy.deepcopy(x['flexible_spec']))
	else:
		if x.get('flexible_spec'):
			targetingSpec['flexible_spec'] = copy.deepcopy(x['flexible_spec'])
	del x

def processExcludes(excludeDict, value, targetingSpec):
	if excludeDict.get(value):
		includeDetails = excludeDict.get(value)
		for k,v in includeDetails.iteritems():
			if targetingSpec.get('exclusions'):
				exclusionDict = targetingSpec.get('exclusions')
				if exclusionDict.get(k):
					#exclusionDict.get(k).extend(v)
					exclusionDict.get(k).extend(copy.deepcopy(v))
				else:
					#exclusionDict[k] = v
					exclusionDict[k] = copy.deepcopy(v)
			else:
				#targetingSpec['exclusions'] = {k:v}
				targetingSpec['exclusions'] = copy.deepcopy({k:v})


def generateconfigurationAutomationNonTV(values, combkeys, permDict, conditionDict, locationDict, header):
	#configdict = readConfigFile('paramconfig.json')
	configdict = {"currency": "USD", "optimize_for": objectiveMappings[values[0]], "optimization_goal": objectiveMappings[values[0]], "targeting_spec": {}}
	targetingSpec = configdict['targeting_spec']

	#Feeds
	if values[2] == 'Facebook Feeds AND Instagram Feeds':
		targetingSpec['publisher_platforms'] = ["facebook", "instagram"]
		targetingSpec["facebook_positions"] = ["feed"]
	elif values[2] == 'Facebook Feeds':
		targetingSpec['publisher_platforms'] = ["facebook"]
		targetingSpec["facebook_positions"] = ["feed"]
	elif values[2] == 'Instagram Feeds':
		targetingSpec['publisher_platforms'] = ["instagram"]
	elif values[2] == 'Instagram Stories':
		targetingSpec['publisher_platforms'] = ["instagram"]
		targetingSpec["instagram_positions"] = ["story"]

	#Device
	if values[3] == 'Mobile AND Desktop':
		targetingSpec['device_platforms'] = ["mobile", "desktop"]
	elif values[3] == 'Mobile':
		targetingSpec['device_platforms'] = ["mobile"]
	elif values[3] == 'Desktop':
		targetingSpec['device_platforms'] = ["desktop"]

	#gender is all be default
	if combkeys[-1] in ('Men', 'Male'):
		targetingSpec['genders'] = [1]
	elif combkeys[-1] in ('Women', 'Female'):
		targetingSpec['genders'] = [2]
	elif combkeys[-1] == 'All':
		if targetingSpec.get('genders'):
                	del targetingSpec['genders']

	#Ages
	#targetingSpec['age_min'],targetingSpec['age_max'] = values[12].split('-')
	targetingSpec['age_min'],targetingSpec['age_max'] = combkeys[-2].split('-')

	#Location
	#if  values[13] == 'USA':
	if  combkeys[0] == 'USA':
		targetingSpec['geo_locations'] = {'countries':['US']}

	#elif locationDict.get(values[13]):
	elif locationDict.get(combkeys[0]):
		#if 'exclude' in values[13].lower():
		if 'exclude' in combkeys[0].lower():
			targetingSpec['geo_locations'] = {'countries':['US']}
			#locationDetails = locationDict.get(values[13])
			locationDetails = locationDict.get(combkeys[0])
			for k,v in locationDetails.iteritems():
				targetingSpec['excluded_geo_locations']= {k:v}

		#elif locationDict.get(values[13]):
		elif locationDict.get(combkeys[0]):
			#locationDetails = locationDict.get(values[13])
			locationDetails = locationDict.get(combkeys[0])
			for k,v in locationDetails.iteritems():
				targetingSpec['geo_locations']= {k:v}


	#Perms
	for elements in  combkeys[1]:
		processIncludesNonTV(permDict, elements, targetingSpec)
	#Conditons
	for elements in  combkeys[2:-2]:
		if  'include' in elements.lower():
			processIncludesNonTV(conditionDict, elements, targetingSpec)
		elif  'exclude' in elements.lower():
			processExcludes(conditionDict, elements, targetingSpec)

	print 'Debug'
	print 'configdict'
	print configdict
	return configdict

def generateconfigurationAutomation(values, includeDict, excludeDict, locationDict, header):
	#configdict = readConfigFile('paramconfig.json')
	configdict = {"currency": "USD", "optimize_for": objectiveMappings[values[0]], "optimization_goal": objectiveMappings[values[0]], "targeting_spec": {}}
	targetingSpec = configdict['targeting_spec']

	#Feeds
	if values[2] == 'Facebook Feeds AND Instagram Feeds':
		targetingSpec['publisher_platforms'] = ["facebook", "instagram"]
		targetingSpec["facebook_positions"] = ["feed"]
	elif values[2] == 'Facebook Feeds':
		targetingSpec['publisher_platforms'] = ["facebook"]
		targetingSpec["facebook_positions"] = ["feed"]
	elif values[2] == 'Instagram Feeds':
		targetingSpec['publisher_platforms'] = ["instagram"]
	elif values[2] == 'Instagram Stories':
		targetingSpec['publisher_platforms'] = ["instagram"]
		targetingSpec["instagram_positions"] = ["story"]

	#Device
	if values[3] == 'Mobile AND Desktop':
		targetingSpec['device_platforms'] = ["mobile", "desktop"]
	elif values[3] == 'Mobile':
		targetingSpec['device_platforms'] = ["mobile"]
	elif values[3] == 'Desktop':
		targetingSpec['device_platforms'] = ["desktop"]

	#gender is all be default
	if values[11] == 'Men':
		targetingSpec['genders'] = [1]
	elif values[11] == 'Women':
		targetingSpec['genders'] = [2]
	elif values[11] == 'All':
		if targetingSpec.get('genders'):
                	del targetingSpec['genders']

	#Ages
	targetingSpec['age_min'],targetingSpec['age_max'] = values[12].split('-')

	#Location
	if  values[13] == 'USA':
		targetingSpec['geo_locations'] = {'countries':['US']}

	elif locationDict.get(values[13]):
		if 'exclude' in values[13].lower():
			targetingSpec['geo_locations'] = {'countries':['US']}
			locationDetails = locationDict.get(values[13])
			for k,v in locationDetails.iteritems():
				targetingSpec['excluded_geo_locations']= {k:v}

		elif locationDict.get(values[13]):
			locationDetails = locationDict.get(values[13])
			for k,v in locationDetails.iteritems():
				targetingSpec['geo_locations']= {k:v}


	#Inclusions
	includeIndexes = getIndexesExcludeIndexName(header, 'Include')
	for elements in  includeIndexes:
		processIncludes(includeDict, values[elements], targetingSpec)

	#Exclusions
	excludeIndexes = getIndexesExcludeIndexName(header, 'Exclude')
	for elements in  excludeIndexes:
		processExcludes(excludeDict, values[elements], targetingSpec)

	print 'Debug'
	print 'configdict'
	print configdict
	return configdict

def generateconfigurationAutomationStep4(values, includeDict, excludeDict, locationDict, permDict, conditionDict,header):
	#configdict = readConfigFile('paramconfig.json')
	configdict = {"currency": "USD", "optimize_for": objectiveMappings[values[0]], "optimization_goal": objectiveMappings[values[0]], "targeting_spec": {}}
	targetingSpec = configdict['targeting_spec']

	#Feeds
	if values[2] == 'Facebook Feeds AND Instagram Feeds':
		targetingSpec['publisher_platforms'] = ["facebook", "instagram"]
		targetingSpec["facebook_positions"] = ["feed"]
	elif values[2] == 'Facebook Feeds':
		targetingSpec['publisher_platforms'] = ["facebook"]
		targetingSpec["facebook_positions"] = ["feed"]
	elif values[2] == 'Instagram Feeds':
		targetingSpec['publisher_platforms'] = ["instagram"]
	elif values[2] == 'Instagram Stories':
		targetingSpec['publisher_platforms'] = ["instagram"]
		targetingSpec["instagram_positions"] = ["story"]

	#Device
	if values[3] == 'Mobile AND Desktop':
		targetingSpec['device_platforms'] = ["mobile", "desktop"]
	elif values[3] == 'Mobile':
		targetingSpec['device_platforms'] = ["mobile"]
	elif values[3] == 'Desktop':
		targetingSpec['device_platforms'] = ["desktop"]

	#gender is all be default
	if values[11] == 'Men':
		targetingSpec['genders'] = [1]
	elif values[11] == 'Women':
		targetingSpec['genders'] = [2]
	elif values[11] == 'All':
		if targetingSpec.get('genders'):
                	del targetingSpec['genders']

	#Ages
	targetingSpec['age_min'],targetingSpec['age_max'] = values[12].split('-')

	#Location
	if  values[13] == 'USA':
		targetingSpec['geo_locations'] = {'countries':['US']}

	elif locationDict.get(values[13]):
		if 'exclude' in values[13].lower():
		#if 'exclude' in combkeys[0].lower():
			targetingSpec['geo_locations'] = {'countries':['US']}
			locationDetails = locationDict.get(values[13])
			for k,v in locationDetails.iteritems():
				targetingSpec['excluded_geo_locations']= {k:v}

		elif locationDict.get(values[13]):
			locationDetails = locationDict.get(values[13])
			for k,v in locationDetails.iteritems():
				targetingSpec['geo_locations']= {k:v}


	#Inclusions
	includeIndexes = getIndexesExcludeIndexNameStep4(header, 'Include')
	for elements in  includeIndexes:
		#processIncludes(includeDict, values[elements], targetingSpec)
		processIncludesNonTV(includeDict, values[elements], targetingSpec)

	#Exclusions
	excludeIndexes = getIndexesExcludeIndexNameStep4(header, 'Exclude')
	for elements in  excludeIndexes:
		processExcludes(excludeDict, values[elements], targetingSpec)
	#Perms
	permIndexes = getIndexesExcludeIndexNameStep4(header, 'permutation')
	for elements in  permIndexes:
		permute = values[elements].split('&')
		for perm in permute:
                	processIncludesNonTV(permDict, perm, targetingSpec)
        #Conditons
	condIndexes = getIndexesExcludeIndexNameStep4(header, 'condition')
	for elements in  condIndexes:
                if  'include' in values[elements].lower():
                        processIncludesNonTV(conditionDict, values[elements], targetingSpec)
                elif  'exclude' in values[elements].lower():
                        processExcludes(conditionDict, values[elements], targetingSpec)
	print 'Debug'
	print 'configdict'
	print configdict
	return configdict

def deliveryEstimateAutomation(payload):
	#del payload['targeting_spec']['exclusions']
	payload['targeting_spec'] = json.dumps(payload['targeting_spec'])
	payload['access_token'] = config['access_token']
	#print payload
	delivery = getDeliverEstimate(payload)
	print delivery
	delivery = [str(x) for x in delivery]
	reach  = str(getReachEstimate(payload))
	print reach
	result = '\t'.join([reach, '\t'.join(delivery)])
	return result

def generateconfiguration(values):
	configdict = readConfigFile('paramconfig.json')
	targetingSpec = configdict['targeting_spec']
	if values[0] == 'Facebook Feeds AND Instagram Feeds':
		targetingSpec['publisher_platforms'] = ["facebook", "instagram"]
		targetingSpec["facebook_positions"] = ["feed"]
	if values[0] == 'Facebook Feeds':
		targetingSpec['publisher_platforms'] = ["facebook"]
		targetingSpec["facebook_positions"] = ["feed"]
	if values[0] == 'Instagram Feeds':
		targetingSpec['publisher_platforms'] = ["instagram"]

	#gender is all be default
	if values[1] == 'Men':
		targetingSpec['genders'] = [1]
	elif values[1] == 'Women':
		targetingSpec['genders'] = [2]
	elif values[1] == 'All':
                del targetingSpec['genders']

	#Ages
	targetingSpec['age_min'],targetingSpec['age_max'] = values[2].split('-')

	#Location
	if  values[3] == 'USA':
		targetingSpec['geo_locations'] = {'countries':['US']}
	elif values[3] == 'Top DMA':
		targetingSpec['geo_locations']= {'geo_markets': dmaList }
	elif values[3] == 'USA Exclude Top DMA':
		targetingSpec['geo_locations'] = {'countries':['US']}
		targetingSpec['excluded_geo_locations']= {'geo_markets': dmaList }

	#Shows
	if values[4] == 'Top IT':
		itinterestdict = {'interests': topITList} 
		targetingSpec['flexible_spec'] = [itinterestdict]
	elif values[4] == 'Sundance':
		suinterestdict = {'interests': sundanceList} 
		targetingSpec['flexible_spec'] = [suinterestdict]
	elif values[4] == 'Top OT':
		otinterestdict = {'interests': topOTList} 
		targetingSpec['flexible_spec'] = [otinterestdict]

	#ExcludeShows
        if values[5] == 'Exclude Top IT':
		exitinterestdict = {'interests': copy.deepcopy(topITList)}
		targetingSpec['exclusions'] =  exitinterestdict
	elif values[5] == 'Exclude Top IT + Sundance':
		localist = topITList + sundanceList 
		exsuinterestdict = {'interests': copy.deepcopy(localist)}
		targetingSpec['exclusions'] =  exsuinterestdict
	
	
	#ExcludeOTT
	if values[6] == 'Exclude Amazon':
		if targetingSpec.get('exclusions'):
			targetingSpec['exclusions']['interests'].append(excludeOTTList[0])
		else:
			examainterestdict = {'interests': [excludeOTTList[0]]}
                	targetingSpec['exclusions'] =  examainterestdict
	elif values[6] == 'Exclude Hulu':
		if targetingSpec.get('exclusions'):
                        targetingSpec['exclusions']['interests'].append(excludeOTTList[1])
                        targetingSpec['exclusions']['interests'].append(excludeOTTList[2])
                else:
                        hinterestdict = {'interests': [excludeOTTList[1], excludeOTTList[2]]}
                        targetingSpec['exclusions'] =  hinterestdict

	elif values[6] == 'Exclude Netflix':
		if targetingSpec.get('exclusions'):
                        targetingSpec['exclusions']['interests'].append(excludeOTTList[3])
                else:
                        ninterestdict = {'interests': [excludeOTTList[3]]}
                        targetingSpec['exclusions'] =  ninterestdict
	return configdict
'''
readDMADetails('topDMA.txt')
topITList = readAdInterestDetails('topit.txt')
sundanceList = readAdInterestDetails('sundance.txt')
topOTList = readAdInterestDetails('topot.txt')
excludeOTTList = readAdInterestDetails('excludeOTT.txt')
'''

def readPayLoads():
	header, printHeader, dictDetails = readStructureConfig('HapLeonard_FB.csv')
	return dictDetails
	 
	
		
if __name__ == "__main__":
	readDMADetails('topDMA.txt')
	topITList = readAdInterestDetails('topit.txt')
	sundanceList = readAdInterestDetails('sundance.txt')
	topOTList = readAdInterestDetails('topot.txt')
	excludeOTTList = readAdInterestDetails('excludeOTT.txt')
	
	header, printHeader, dictDetails = readStructureConfig('HapLeonard_FB.csv')
	resultheader = 'AduienceCount\tEstimate_DAU\tEstimate_MAU\tMinBid\tMedianBid\tMaxBid\n'
	f = open('HapLeonardLinear.csv', 'w')
	f.write(header+'\t'+resultheader)
	for key in printHeader:
		values = dictDetails[key]
		print values
		payload = generateconfiguration(values)
		payload['targeting_spec'] = json.dumps(payload['targeting_spec'])
		payload['access_token'] = config['access_token']
		#print payload
		delivery = getDeliverEstimate(payload)
		print delivery
		delivery = [str(x) for x in delivery]
		reach  = str(getReachEstimate(payload))
		print reach
	        result = '\t'.join([key.split('_')[0], key, '\t'.join(values), reach, '\t'.join(delivery)])
		f.write(result+'\n')
	f.close()
