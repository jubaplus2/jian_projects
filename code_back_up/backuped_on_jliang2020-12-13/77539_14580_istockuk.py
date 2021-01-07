from nltk.util import ngrams
import pandas as pd
import re
import nltk
from collections import Counter
'''
sentence = 'foo bar baz qux'
adj = [3, 2, 1]
for n in adj:
	print ngrams(sentence.split(), n)
'''
#Words to be considered photos,illustrations,videos,audio


#discard https,pricing,plans, sign, join, word sem discard, forum exclude help, images
#excludeworldlist = ['https', 'pricing', 'plans', 'sign', 'join', 'sem', 'forum', 'help', 'images']
excludeworldlist = ['https', 'pricing', 'plans', 'sign', 'join', 'sem', 'forum', 'help']

def excludesearch(inputstring):
	for elements in excludeworldlist:
		if elements in inputstring:
			return True
	return False

from nltk.corpus import stopwords
cachedStopWords = set(stopwords.words("english"))

#Below words are not considered for the word phrases
wordstoberemoved = cachedStopWords | set(['http', 'www', 'istockphoto', 'com', 'php'])
from nltk.corpus import words
validwordlist = words.words()
pstemmer = nltk.PorterStemmer()
lstemmer = nltk.LancasterStemmer()
wnl = nltk.WordNetLemmatizer()

#Segement list
segementList = ['photo', 'illustration', 'video', 'audio']

def getWordsFromURL(input):
	#re.compile('[A-Za-z]+').findall(x)
	#return list(set(re.compile('\w+').findall(input)) - wordstoberemoved)
	#Excluding numbers
	lower = [elements.lower() for elements in re.compile('[A-Za-z]+').findall(input)]
	vv = list(set(lower) - wordstoberemoved)
	#vv = list(set(re.compile('[A-Za-z]+').findall(input)) - wordstoberemoved)
	#All words of length <= 1 is discarded and considered as ''
	return [x if len(x) > 1 else '' for x in vv]

def checkValidWords(input):
	if input in  validwordlist:
		return True
	if pstemmer.stem(input) in validwordlist:
		return True
	if lstemmer.stem(input) in validwordlist:
		return True
	if wnl.lemmatize(input) in validwordlist:
		return True
	return False
	

def validWord(inputlist):
	wordlist = []
	for elements in inputlist:
		#elements = elements.lower()
		#if elements in validwordlist:
		if checkValidWords(elements):
			wordlist.append(elements)
	return wordlist

def getSegmentDetails(url):
	lurl = url.split('/')[3].lower()
	if lurl in segementList:
		return lurl
	v = pstemmer.stem(lurl)
	if v in segementList:
		return v
	v = lstemmer.stem(lurl)
	if v in segementList:
		return v
	v = wnl.lemmatize(lurl)
	if v in segementList:
		return v
	return 'others'


def geturltoworddict(vv):
	urltoworddict = {}
	for x in vv:
		segmentdetailsvalue = getSegmentDetails(x[0])
		if not urltoworddict.get(segmentdetailsvalue):
			#urltoworddict[segmentdetailsvalue] = Counter()
			#dict of list where first elem is the freq, others cols till 2~4x    5x+     cart    Gross ID count  Unique ID count
			urltoworddict[segmentdetailsvalue] = {} 
		wordlist = validWord(getWordsFromURL(x[0]))
		counter = 1 
		while counter <= len(wordlist):
			vv = ngrams(wordlist, counter)
			for elements in vv:
				#urltoworddict[segmentdetailsvalue][elements]+=1
				if urltoworddict[segmentdetailsvalue].get(elements):
					values = urltoworddict[segmentdetailsvalue][elements]
					values[0]+=1
					for i in xrange(1, len(x)):
						values[i]+=x[i]
				else:
					values = [1,x[1],x[2],x[3],x[4],x[5]]
				urltoworddict[segmentdetailsvalue][elements] = values
				
			counter+=1
		
	return urltoworddict

def removenoiseurl(v):
	t = list(set(v['URL']))
	vv = []
	for x in t:
        	if not excludesearch(x):
                	vv.append(x)
	return vv

def removenoiseurl1(v):
	count = 0
	vv = []
	while count < len(v):
		if not excludesearch(v['URL'][count]):
			re = []
			re.append(v.iloc[count, 0])
			re.extend(v.iloc[count, 2:7])
			vv.append(re)
			#vv.append([v['URL'][count], v['2~4x'][count], v['5x+'][count], v['cart'][count], v['Gross ID count'][count],
			#		v['Unique ID count'][count]])
		count+=1
	return vv

def calculateWordFreq(urltoworddict):
	wordlist = []
	for value in urltoworddict.values():
		wordlist.extend(value)
	wordlistfreq = Counter(wordlist)
	return wordlistfreq

		

if __name__ == "__main__":
	v = pd.read_csv('/home/account1/Gettysegmentation/iStockUKURL.txt', '\t')
	#v = pd.read_csv('nlptest.csv', '\t')
	tt = removenoiseurl1(v)
	urltoworddict = geturltoworddict(tt)
	with open('iStockUKURLOutputCart.csv','w') as f:
		for k,v in urltoworddict.iteritems():
			totalfreq = sum(values[0] for key,values in v.iteritems())
			f.write(k+'\t'+str(totalfreq)+'\n')
			#t = sorted(v.keys(), key =lambda x: v[x][0])
			for key,values in v.iteritems():
				values = '\t'.join([str(i) for i in values])
				f.write(str(key)+'\t'+values+'\n\n')
			#for key in t:
				#f.write(str(key)+'\t'+str(v[key])+'\n\n')
	print 'arnab'