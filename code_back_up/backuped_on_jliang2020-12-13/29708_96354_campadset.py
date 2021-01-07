import requests
import json
import os
import sys
#from estimatecal import getsofarListDetails, writeToList
import copy
#from structureparser import readPayLoads, generateconfiguration
import time

api_version = 'v2.11'
timestr = time.strftime("%Y%m%d-%H%M%S")

this_dir = os.path.dirname(__file__)
config_filename = os.path.join(this_dir, '../config/config.json')
config_file = open(config_filename)
config = json.load(config_file)
config_file.close()

def getcampaignID():
	campaignId = ''
	try:
		fl1 = open('campaignid.txt','r')
		campaignId= fl1.read()  
		fl1.close()
		return campaignId
	except Exception:
		pass
	return campaignId

def scriptUsageDetails():
	print 'Script Usage:'
	print 'Campaign Creation : python campaignoperation.py create inputfile.csv'
	print 'Campaign Update : python campaignoperation.py update campaignid inputfile.csv'
	print 'Campaign Read :python campaignoperation.py read campaignid'
	print 'Campaign Deletion :python campaignoperation.py delete campaignid'
	print 'Campaign/Adset Deletion from a file :python campaignoperation.py deletef'

def readinputFile(fileName):
	resultDict = {}
	with open(fileName, 'r') as f:
		details = f.read().split('\n')
		#First line of the input file is header details so excluding that
		details.remove('')
		details = details[1:]
		for elements in details:
			key,value = elements.split('\t')
			resultDict[key.strip()] = value.strip()
	return resultDict

def errorDetails(response):
	print 'Error =%s' %str(response.text)
        print 'Code =%s' %str(response.status_code)

def deleteCampaign(campaignId):
	payload = {'access_token':config['access_token']}
	deleteUrl = 'https://graph.facebook.com/'+api_version+'/'+campaignId+'/'
	response = requests.delete(deleteUrl, data=payload)
	if response.status_code == 200:
		print 'Node ID Deleted =%s' %str(campaignId)
	else:
		errorDetails(response)

def deleteCampaignF(inputfile):
	#f = open('AdsetDetailsToBeDelete.txt','r')
	f = open(inputfile,'r')
	t = f.read()
	t1 = t.split('\n')
	if '' in t1:
		t1.remove('')
	for elements in t1:
		values = elements.split('\t')
		if '\r' in values[-1]:
			values[-1] = values[-1].strip('\r')
		if values[-1] == '1':
			deleteCampaign(values[0])
	f.close()


def createCampaign(payload):
	requestUrl = 'https://graph.facebook.com/'+api_version+'/'+config['act_id']+'/'+'campaigns'
	payload['access_token'] = config['access_token']
	response = requests.post(requestUrl, data=payload)
	if response and response.status_code == 200:
		jsondict = json.loads(response.text)
		print 'Campaign ID Created=%s' %str(jsondict['id'])
		return str(jsondict['id'])
	else:
		errorDetails(response)

def updateCampaign(campaignId, payload):
	requestUrl = 'https://graph.facebook.com/'+api_version+'/'+campaignId+'/'
	payload['access_token'] = config['access_token']
	response = requests.post(requestUrl, data=payload)
	if response and response.status_code == 200:
		jsondict = json.loads(response.text)
		print 'Campaign ID Updated=%s Success=%s' %(campaignId, str(jsondict['success']))
	else:
		errorDetails(response)

def readCampaign(campaignId):
	requestUrl = 'https://graph.facebook.com/'+api_version+'/'+campaignId+'/'
	readFields = ['id', 'name', 'objective', 'account_id', 'adlabels', 'boosted_object_id', 
                      'brand_lift_studies', 'budget_rebalance_flag', 'buying_type', 'can_create_brand_lift_study', 
                      'can_use_spend_cap', 'configured_status', 'created_time', 'effective_status', 'kpi_custom_conversion_id', 
                      'kpi_type', 'recommendations', 'source_campaign', 'source_campaign_id', 'spend_cap', 'start_time', 
                      'status', 'stop_time', 'updated_time', 'ad_studies', 'ads', 'adsets', 'copies', 'insights']

	payload = {'access_token':config['access_token']}
        payload['fields'] = ','.join(readFields)
	response = requests.get(requestUrl, params=payload)
	if response and response.status_code == 200:
                print 'Campaign ID Read=%s Results=%s' %(campaignId, str(response.text))
        else:
                errorDetails(response)


def readConfigFile(inputFile):
        with open(inputFile, 'r') as f:
                t = f.read()
		t = t.replace('\n','')
                configdict = json.loads(t)
        return configdict


def createAdsetPayload(inputfile):
        payload = readConfigFile(inputfile)
        payload['access_token'] = config['access_token']
        #payload['targeting_spec'] = json.dumps(payload['targeting_spec'])
        return payload

def createAdset(payload, campaignid):
	payload['campaign_id'] = campaignid
	v = payload['targeting']
	payload['targeting'] = json.dumps(v)
        requestUrl = 'https://graph.facebook.com/'+api_version+'/'+config['act_id']+'/'+'adsets'
        response = requests.post(requestUrl, data=payload)
        if response and response.status_code == 200:
                jsondict = json.loads(response.text)
                print 'Adset ID Created=%s' %str(jsondict['id'])
                return str(jsondict['id'])
        else:
                errorDetails(response)
		return 0

def createAutomationCampaign(inputfile):
	payload = readinputFile(inputfile)
	print 'Creating campaign with following values = %s' %str(payload)
	campaignid = getcampaignID()
	if len(campaignid)  == 0:
		result = createCampaign(payload)
		print 'Campaign id %s is created' %campaignid
		fl1 = open('campaignid.txt','w')
		fl1.write(result)
		fl1.close()

	else:
		print 'Campaign id %s is present in campaignid.txt so all adsets are created under same campaign' %campaignid
		result = campaignid
	return result



if __name__ == "__main__":
	if len(sys.argv) > 2:
		if sys.argv[1] not in ('create', 'update', 'read', 'delete', 'deletef'):
			scriptUsageDetails()

		elif sys.argv[1] == 'create':
			payload = readinputFile(sys.argv[2])
			print 'Creating campaign with following values = %s' %str(payload)
			campaignid = getcampaignID()
			if len(campaignid)  == 0:
        			result = createCampaign(payload)
				print 'Campaign id %s is created' %campaignid
				fl1 = open('campaignid.txt','w')
        			fl1.write(result)
        			fl1.close()

			else:
				print 'Campaign id %s is present in campaignid.txt so all adsets are created under same campaign' %campaignid
				result = campaignid
			
			payload = readConfigFile('paramconfig.json')
			valuetargeting_spec = copy.deepcopy(payload['targeting_spec'])
			print valuetargeting_spec
			lsofarList = getsofarListDetails()
                        newAdCreation = 0
			dictDetails = readPayLoads()
			fileName = 'AdsetDetails_'+timestr+'.txt'
			fadset = open(fileName, 'w')
			fadset.write('AdsetID\tAdsetName\n')
			for key, values in dictDetails.iteritems():
				payload = generateconfiguration(values)
				if json.dumps(payload['targeting_spec']) not in lsofarList:
					print 'adsetCreation'
					payload['targeting'] = payload['targeting_spec']
					payload['name'] = key
					payload['billing_event'] = 'IMPRESSIONS'
					#payload['bid_amount'] = '2'
					payload['daily_budget'] = '1000'
					#payload['lifetime_budget'] = '1000'
					payload['is_autobid'] = 'TRUE'
					payload['start_time'] = '2018-02-10 10:00:00 EST'
					payload['end_time']  = '2018-02-11 21:00:00 EST'
					payload['status']= 'PAUSED'
					del  payload['targeting_spec']
					payload['access_token'] = config['access_token']
					adsetid = createAdset(payload, result)
					if adsetid != 0:
						print 'AdsetName=%s' %key
						writeToList(payload)
						fadset.write(adsetid+'\t'+key+'\n')
					newAdCreation = 1
				else:
					print 'Adset not created as entry is there in listsofar.txt'
			if newAdCreation == 0:
				print 'All Ad creation completed'

		elif sys.argv[1] == 'update':
			payload = readinputFile(sys.argv[3])
			print 'Updating campaignid = %s  with following values = %s' %(sys.argv[2], str(payload))
        		result = updateCampaign(sys.argv[2],payload)

		elif sys.argv[1] == 'read':
			readCampaign(sys.argv[2])

		elif sys.argv[1] == 'delete':
			deleteCampaign(sys.argv[2])

		elif sys.argv[1] == 'deletef':
			deleteCampaignF(sys.argv[2])
			
			
        else:
		scriptUsageDetails()
