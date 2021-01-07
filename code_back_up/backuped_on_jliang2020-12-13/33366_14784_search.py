import requests
import json
import unicodedata
import config
access = 'EAACuKss3go0BAPp1ayGisyVftbBAiiML3JOEM8CZCOEk98PZA3lwt4Li5fkiJFYEUECsMX5lPMeDqqRzk60ZCiRRKUtIrZCvWi47uRfNJ0SRVZCwcFLwqcpcAPEqViQM4uS8mmGNCLC155dizJpEtZBQedKPbSLCFfwJejPNlXiJah74LTNaOojcXALGI8emUZD'
requestUrl = 'https://graph.facebook.com/v2.11/search'
requestUrl = 'https://graph.facebook.com/'+ config.api_version+'/search'

def searchQuery(payload, queryString):
        response = requests.get(requestUrl, params=payload)
        if response.status_code == 200:
                dataDictList = json.loads(response.text)
                dataDictList = dataDictList['data']
                for elements in dataDictList:
                        #if elements['name'] == 'Blender':
                        compareString = unicodedata.normalize('NFKD', elements['name']).encode('ascii','ignore')
                        #if elements['name'] == queryString:
                        if compareString == queryString:
                                #v = json.dumps({ "id": elements['id'], "name": elements['name'] })
                                v = { "id": elements['id'], "name": elements['name']}
                                return v
        else:
                print 'error'
                print response.text
        return ''

def targetSearch(queryString, payloadtype):
        v = payloadtype.split(':')
        payload = {}
        payload = {'access_token':config.config['access_token']}
        t = queryString.split(' ')
	#Taking care of 's in Master's degree
	if '\'s' in t[0]:
		t[0] = t[0].strip('\'s')
        payload['q'] = t[0]
        targetSearchUrl = 'https://graph.facebook.com/'+ config.api_version +'/'+ config.config['act_id']+ '/targetingsearch'
        response = requests.get(targetSearchUrl, params=payload)
        if response.status_code == 200:
                dataDictList = json.loads(response.text)
                dataDictList = dataDictList['data']
                for elements in dataDictList:
                        compareString = unicodedata.normalize('NFKD', elements['name']).encode('ascii','ignore')
                        if compareString.lower() == queryString.lower():
				#targetingsearch:education_statuses
				if len(v) > 1:
					if v[1].lower() == elements['type']:
                                		return { "id": elements['id'], "name": elements['name'], "type": elements['type']}
				else:
                                		return { "id": elements['id'], "name": elements['name'], "type": elements['type']}
        else:
                print 'error'

def searchString(queryString, payloadtype):
        payload = {}
        #payload = {'access_token':access}
        payload = {'access_token':config.config['access_token']}
        payload['limit'] = 1000
        #payload['q'] = 'Blender'
        t = queryString.split(' ')
        #if t[0] != 'The':
        if t[0] not in ['The', 'Free']:
                payload['q'] = t[0]
        else:
                payload['q'] = t[1]
        payload['type'] = payloadtype
        queryResult = searchQuery(payload, queryString)
        if queryResult == '':
                #Trying with the last word
                payload['q'] = t[-1]
                queryResult = searchQuery(payload, queryString)
        return queryResult

#'adTargetingCategory:class=income' example of the queryString
def searchAdTargetingCategory(payloadtype):
	v = payloadtype.split(':')
	payload = {}
	payload = {'access_token':config.config['access_token']}
	payload['type'] =v[0]
	payload['class'] = v[1].split('=')[1]
	payload['limit'] = 1000
	response = requests.get(requestUrl, params=payload)
	returnList = []
        if response.status_code == 200:
                dataDictList = json.loads(response.text)
                dataDictList = dataDictList['data']
                for elements in dataDictList:
                        compareString = unicodedata.normalize('NFKD', elements['name']).encode('ascii','ignore')
                        returnList.append({ "id": elements['id'], "name": elements['name']})
        else:
                print 'error'
        return returnList
	

def searchIncome(queryString, payloadtype):
	incomeDetailsList =  searchAdTargetingCategory(payloadtype)
	for elements in incomeDetailsList:
		if elements['name'] == queryString:	
			return elements
	return None

def searchIncome(queryString, payloadtype):
        incomeDetailsList =  searchAdTargetingCategory(payloadtype)
        for elements in incomeDetailsList:
                if elements['name'] == queryString:
                        return elements
        return None

def searchBehaviors(queryString, payloadtype):
	incomeDetailsList =  searchAdTargetingCategory(payloadtype)
	for elements in incomeDetailsList:
		#Arnab: Input file was having African American but from API it returned African American (US)
		#so doing a length match only
		if elements['name'][0:len(queryString)] == queryString:	
			return elements
	return None

def searchDMA(queryString, payloadtype):
	v = payloadtype.split(':')
        payload = {}
        payload = {'access_token':config.config['access_token']}
        payload['type'] =v[0]
	payload['location_types'] = v[1]
        payload['limit'] = 1000
	if '"' in queryString:
		queryString = queryString.strip('"')
	t = queryString.split(' ')
	payload['q'] = t[0]
        response = requests.get(requestUrl, params=payload)
        if response.status_code == 200:
                dataDictList = json.loads(response.text)
                dataDictList = dataDictList['data']
                for elements in dataDictList:
                        compareString = unicodedata.normalize('NFKD', elements['name']).encode('ascii','ignore')
			if compareString.lower() == queryString.lower():
                        	return { "key": elements['key'], "name": elements['name']}
        else:
                print 'error'

def searchRegions(queryString, payloadtype):
	v = payloadtype.split(':')
        payload = {}
        payload = {'access_token':config.config['access_token']}
        payload['type'] =v[0]
	payload['location_types'] = v[1]
        payload['limit'] = 1000
	t = queryString.split(' ')
	payload['q'] = t[0]
        response = requests.get(requestUrl, params=payload)
        if response.status_code == 200:
                dataDictList = json.loads(response.text)
                dataDictList = dataDictList['data']
                for elements in dataDictList:
                        compareString = unicodedata.normalize('NFKD', elements['name']).encode('ascii','ignore')
			if compareString == queryString:
                        	return { "key": elements['key'], "name": elements['name']}
        else:
                print 'error'
'''
f = open('inputsearch.txt')
t = f.read()
v = t.split('\n')
v.remove('')
result = []
searchtype = 'adinterest'
for elements in v:
	result.append(searchString(elements, 'adinterest'))
f.close()
if result:
	print result
	if '' in result:
		result.remove('')
	f = open('output.txt', 'w')
	f.write(searchtype+'\n')
	f.write('\n'.join(result))
	f.close()
'''
