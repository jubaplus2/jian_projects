import requests
import json
import os
import sys
import itertools
import time
from itertools import combinations
import copy
#from configreader import configDict,combList
import time

api_version = 'v2.11'

this_dir = os.path.dirname(__file__)
config_filename = os.path.join(this_dir, '../config/config.json')
config_file = open(config_filename)
config = json.load(config_file)
config_file.close()
'''
timestr = time.strftime("%Y%m%d-%H%M%S")
outputfileName = 'output/'+'Report.'+timestr+'.csv'
f = open(outputfileName,'w')
header = 'Ids\tTargetSpec\tFlexibleSpec\tAduienceCount\tEstimate_DAU\tEstimate_MAU\tMinBid\tMedianBid\tMaxBid\tUniverse\n'
f.write(header)
'''

fl = open('listsofar.txt','a+')
sofarList = []

def writeToList(payload):
	if payload.get('targeting_spec'):
        	fl.write(payload['targeting_spec']+'\n')
	else:
        	fl.write(payload['targeting']+'\n')

def getsofarListDetails():
	global sofarList
	fl1 = open('listsofar.txt','r')
	t = fl1.read()
	if t:
		sofarList = t.split('\n')
		sofarList.remove('')
	return sofarList

def errorDetails(response):
	print 'Error =%s' %str(response.text)
        print 'Code =%s' %str(response.status_code)


def readConfigFile(inputFile):
	with open(inputFile, 'r') as f:
		t = f.read()
		configdict = json.loads(t)
	return configdict


def processRequest(requestUrl, payload):
	response = requests.get(requestUrl, params=payload)
        if response and response.status_code == 200:
                resultdict = json.loads(response.text)
                return resultdict
        else:
                errorDetails(response)

def getReachEstimate(payload):
	requestUrl = 'https://graph.facebook.com/'+api_version+'/'+config['act_id']+'/'+'reachestimate'
        resultdict = processRequest(requestUrl, payload)
	if resultdict:
		return resultdict['data']['users']


def getDeliverEstimate(payload):
	requestUrl = 'https://graph.facebook.com/'+api_version+'/'+config['act_id']+'/'+'delivery_estimate'
        resultdict = processRequest(requestUrl, payload)
	if  resultdict:
		values = resultdict['data'][0]
		biddetails = values['bid_estimate']
		result = (values['estimate_dau'], values['estimate_mau'], biddetails['min_bid'], biddetails['median_bid'], biddetails['max_bid'])
		return result

def createPayload(inputfile):
	payload = readConfigFile('paramconfig.txt')
	payload['access_token'] = config['access_token']
	payload['targeting_spec'] = json.dumps(payload['targeting_spec'])
	return payload


def combinationSingleList(interestdetails):
	interestlen = len(interestdetails)	
	t = [list(combinations(interestdetails, x)) for x in range(1,interestlen+1)]
	return t

def combinationInterestsBehavior(interestdetails, behvariordetails):
        finalList = []
        if interestdetails and behvariordetails:
		finalList  = interestdetails+ behvariordetails
        elif interestdetails:
		finalList  = interestdetails
	elif behvariordetails:
		finalList  = behvariordetails
	finalListLen = len(finalList)
	t = [list(combinations(finalList, x)) for x in range(1,finalListLen+1)]
	return t

def dumptofile(inputdetails, universedetails):
	userscount = inputdetails[1]
	deliveryestimates = inputdetails[2]
	estimatedau = deliveryestimates[0]
	estimatemau = deliveryestimates[1]
        minbid = deliveryestimates[2]
        medianbid = deliveryestimates[3]
        maxbid = deliveryestimates[4]
	interestdetails = inputdetails[0]
	statdetails = '\t'.join([str(userscount), str(estimatedau), str(estimatemau), str(minbid), str(medianbid), str(maxbid)])
        flexibleSpec = inputdetails[3]
	t = [e.values()for e in interestdetails]
        ll = [e[0]+':'+e[1] for e in t]
	ids = [str(e.split(':')[0]) for e in ll]
	idDetails = ' | '.join(ids)
	vv = ' & '.join(ll)
	flexvv = []
        if flexibleSpec:
		flexvv = []
		for elements in flexibleSpec:
			flexibleSpecinterest = elements['interests']
			t = [e.values()for e in flexibleSpecinterest]
			ll = [e[0]+':'+e[1] for e in t]
			tt = ' | '.join(ll)
                        flexvv.append(tt)
	if flexvv:
		flexvv = ' & '.join(flexvv)
	else:
		flexvv = ''
	result = '\t'.join([idDetails, vv, flexvv, statdetails, universedetails])+'\n'
	f.write(result)


def getEstimatesIntBeh(inputPayload, inputList, publisherPlatforms):
        lpayload = dict(inputPayload)
        lpayload['access_token'] = config['access_token']
        targetingSpec = inputPayload['targeting_spec']
        geoLocations = targetingSpec['geo_locations']
        ageMin = targetingSpec['age_min']
        ageMax = targetingSpec['age_max']
        del lpayload['targeting_spec']
        counter = 0
	configuredInterestsList = targetingSpec.get('interests')
	configuredBehaviorsList = targetingSpec.get('behaviors')
        targetspecforprint = []
        targetspecforprint = copy.deepcopy(targetingSpec.get('flexible_spec'))
        for inputs in inputList:
                for elements in inputs:
                        interestsValue = []
                        interestsValue.extend([v for v in elements])
                        lpayload['targeting_spec'] = {}
			interestsValueList = []
			behaviorsValuelist = []
			for elements in interestsValue:
				if elements in configuredInterestsList:
					interestsValueList.append(elements)
				elif elements in configuredBehaviorsList:
					behaviorsValuelist.append(elements)
                        if len(interestsValueList)> 0:
                                lpayload['targeting_spec']['interests'] = interestsValueList

                        if len(behaviorsValuelist)> 0:
                                lpayload['targeting_spec']['behaviors'] = behaviorsValuelist

                        lpayload['targeting_spec']['publisher_platforms'] = [publisherPlatforms]
                        lpayload['targeting_spec']['geo_locations'] = geoLocations
                        lpayload['targeting_spec']['age_min'] = ageMin
                        lpayload['targeting_spec']['age_max'] = ageMax
                        if targetingSpec.get('flexible_spec'):
                        	lpayload['targeting_spec']['flexible_spec'] = copy.deepcopy(targetingSpec.get('flexible_spec'))
			else:
				lpayload['targeting_spec']['flexible_spec'] = []
			if lpayload['targeting_spec'].get('interests'):
				del lpayload['targeting_spec']['interests']
			if lpayload['targeting_spec'].get('behaviors'):
				del  lpayload['targeting_spec']['behaviors']
			flexinterestsValueList = None
			flexbehaviorsValuelist = None	
			if interestsValueList:
				flexinterestsValueList = [{'interests':[t]} for t in interestsValueList]
				#flexinterestsValueList = [{'interests':interestsValueList}]
			if behaviorsValuelist:
				flexbehaviorsValueList = [{'behaviors':[t]} for t in behaviorsValuelist]
			if flexinterestsValueList:
				lpayload['targeting_spec']['flexible_spec'].extend(flexinterestsValueList)
			if flexbehaviorsValuelist:
				lpayload['targeting_spec']['flexible_spec'].extend(flexbehaviorsValuelist)
			
                        #print str(lpayload['targeting_spec'])
			print str(lpayload['targeting_spec']['flexible_spec'])
                        lpayload['targeting_spec']= json.dumps(lpayload['targeting_spec'])
			if lpayload['targeting_spec'] not in sofarList:
				#result = (interestsValue, getReachEstimate(lpayload), getDeliverEstimate(lpayload), targetingSpec.get('flexible_spec'))
				result = (interestsValue, getReachEstimate(lpayload), getDeliverEstimate(lpayload), targetspecforprint)
				if result[1] and result[2]:
					dumptofile(result, publisherPlatforms)
					fl.write(lpayload['targeting_spec']+'\n')
				else:
					print 'Limit reached'
					return
			del lpayload['targeting_spec']
			counter+=1
			print 'Value of counter =%d' %counter
        print 'Value of counter =%d' %counter

'''
def arnab(lpayload):
	valuetargeting_spec = copy.deepcopy(lpayload['targeting_spec'])
	f = open('BigLotEstimates.csv', 'w')
	header = 'Gender\tPlacement\tLocation\tAge\tProduct\tFamily Status\tRetailers\tIncome\tAduienceCount\tEstimate_DAU\tEstimate_MAU\tMinBid\tMedianBid\tMaxBid\n'
	f.write(header)
	for elements in combList:
		print elements
		lpayload['targeting_spec']['flexible_spec'] = []
		#lpayload['targeting_spec']['flexible_spec'] = copy.deepcopy(targetingSpec.get('flexible_spec'))

		#getting zip details
		#lpayload['targeting_spec']['geo_locations']['zips'] = configDict[elements[0]][0:1]
		lpayload['targeting_spec']['geo_locations']['zips'] = configDict[elements[0]][0:1500]

		#getting prod details
		lpayload['targeting_spec']['flexible_spec'].append(configDict[elements[1]]['flexible_spec'][0])

		#getting family details
		if 'FamilyWChildren' ==  elements[2]:
			lpayload['targeting_spec']['flexible_spec'].append(configDict[elements[2]]['flexible_spec'][0])

		elif 'FamilyWoChildren' ==  elements[2]:
			if lpayload['targeting_spec'].get('exclusions') is None:
				lpayload['targeting_spec']['exclusions'] = {}
			lpayload['targeting_spec']['exclusions']['family_statuses']  = configDict[elements[2]]['flexible_spec'][0]['family_statuses']

		#getting retailer details
		if 'Retailer' == elements[3]:
			lpayload['targeting_spec']['flexible_spec'].append(configDict[elements[3]]['flexible_spec'][0])

		elif 'ExcludeRetailer' == elements[3]:
			if lpayload['targeting_spec'].get('exclusions') is None:
				lpayload['targeting_spec']['exclusions'] = {}
			lpayload['targeting_spec']['exclusions']['interests']  = configDict[elements[3]]['flexible_spec'][0]['interests']
			#lpayload['targeting_spec']['exclusions']['work_employers']  = configDict[elements[3]]['flexible_spec'][1]['work_employers']
			lpayload['targeting_spec']['exclusions']['work_employers']  = configDict[elements[3]]['flexible_spec'][0]['work_employers']

		#getting income details
		if '25k-75k' == elements[4]:
			lpayload['targeting_spec']['flexible_spec'].append(configDict[elements[4]]['flexible_spec'][0])

		elif 'Exclude25k-75k' == elements[4]:
			if lpayload['targeting_spec'].get('exclusions') is None:
				lpayload['targeting_spec']['exclusions'] = {}
			lpayload['targeting_spec']['exclusions']['income']  = configDict[elements[4]]['flexible_spec'][0]['income']
		lpayload['access_token'] = config['access_token']
		lpayload['targeting_spec'] = json.dumps(lpayload['targeting_spec'])
		#print lpayload['targeting_spec']
		reach  = getReachEstimate(lpayload)
		print reach
		#time.sleep(5)
	        delivery = getDeliverEstimate(lpayload)
		#time.sleep(5)
		print delivery
		delivery = [str(x) for x in delivery]
		del lpayload['targeting_spec']
		lpayload['targeting_spec'] = copy.deepcopy(valuetargeting_spec)
		values = ['Female', 'Facebook:Feeds', elements[0],'35-60',elements[1],elements[2],elements[3], elements[4],
			str(reach), delivery[0], delivery[1], delivery[2], delivery[3], delivery[4]]
		output = '\t'.join(values)
		f.write(output+'\n')
'''
'''
def arnab_f(lpayload, f, elements):
	#valuetargeting_spec = copy.deepcopy(lpayload['targeting_spec'])
	#print elements
	lpayload['targeting_spec']['flexible_spec'] = []
	#lpayload['targeting_spec']['flexible_spec'] = copy.deepcopy(targetingSpec.get('flexible_spec'))

	#getting zip details
	lpayload['targeting_spec']['geo_locations']['zips'] = configDict[elements[0]][0:1500]
	#lpayload['targeting_spec']['geo_locations']['zips'] = configDict[elements[0]][0:1]

	#getting prod details
	lpayload['targeting_spec']['flexible_spec'].append(configDict[elements[1]]['flexible_spec'][0])

	#getting family details
	if 'FamilyWChildren' ==  elements[2]:
		lpayload['targeting_spec']['flexible_spec'].append(configDict[elements[2]]['flexible_spec'][0])

	elif 'FamilyWoChildren' ==  elements[2]:
		if lpayload['targeting_spec'].get('exclusions') is None:
			lpayload['targeting_spec']['exclusions'] = {}
		lpayload['targeting_spec']['exclusions']['family_statuses']  = configDict[elements[2]]['flexible_spec'][0]['family_statuses']

	#getting retailer details
	if 'Retailer' == elements[3]:
		lpayload['targeting_spec']['flexible_spec'].append(configDict[elements[3]]['flexible_spec'][0])

	elif 'ExcludeRetailer' == elements[3]:
		if lpayload['targeting_spec'].get('exclusions') is None:
			lpayload['targeting_spec']['exclusions'] = {}
		lpayload['targeting_spec']['exclusions']['interests']  = configDict[elements[3]]['flexible_spec'][0]['interests']
		#lpayload['targeting_spec']['exclusions']['work_employers']  = configDict[elements[3]]['flexible_spec'][1]['work_employers']
		lpayload['targeting_spec']['exclusions']['work_employers']  = configDict[elements[3]]['flexible_spec'][0]['work_employers']

	#getting income details
	if '25k-75k' == elements[4]:
		lpayload['targeting_spec']['flexible_spec'].append(configDict[elements[4]]['flexible_spec'][0])

	elif 'Exclude25k-75k' == elements[4]:
		if lpayload['targeting_spec'].get('exclusions') is None:
			lpayload['targeting_spec']['exclusions'] = {}
		lpayload['targeting_spec']['exclusions']['income']  = configDict[elements[4]]['flexible_spec'][0]['income']
	lpayload['access_token'] = config['access_token']
	lpayload['targeting_spec'] = json.dumps(lpayload['targeting_spec'])
	if lpayload['targeting_spec'] not in sofarList:
		fl.write(lpayload['targeting_spec']+'\n')
	else:
		return 0
	reach  = getReachEstimate(lpayload)
	#print reach
	delivery = getDeliverEstimate(lpayload)
	#print delivery
	delivery = [str(x) for x in delivery]
	#del lpayload['targeting_spec']
	#lpayload['targeting_spec'] = valuetargeting_spec
	values = ['Female', 'Facebook:Feeds', elements[0],'35-60',elements[1],elements[2],elements[3], elements[4],
		str(reach), delivery[0], delivery[1], delivery[2], delivery[3], delivery[4]]
	output = '\t'.join(values)
	f.write(output+'\n')
	return 1
'''

if __name__ == "__main__":
	#print combList
	#print configDict
	payload = readConfigFile('paramconfig.json')
	arnab(payload)
