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

def additionallist():
        with open('excludewordlist.txt') as f:
                t = f.read().split('\n')
                t.remove('')
                return set(t)


#discard https,pricing,plans, sign, join, word sem discard, forum exclude help, images
#excludeworldlist = ['https', 'pricing', 'plans', 'sign', 'join', 'sem', 'forum', 'help', 'images']
#excludeworldlist = ['https', 'pricing', 'plans', 'sign', 'join', 'sem', 'forum', 'help']
excludeworldlist = list(set(['https', 'pricing', 'plans', 'sign', 'join', 'sem', 'forum', 'help', 'photos', 'video', 'phrase', 'sort', 'true', 'false', 'license', 'creative'])| additionallist())

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
		segmentdetailsvalue = getSegmentDetails(x)
		if not urltoworddict.get(segmentdetailsvalue):
			urltoworddict[segmentdetailsvalue] = Counter()
		wordlist = validWord(getWordsFromURL(x))
		counter = 1 
		while counter <= len(wordlist):
			vv = ngrams(wordlist, counter)
			for elements in vv:
				 urltoworddict[segmentdetailsvalue][elements]+=1
			counter+=1
		
	return urltoworddict

def removenoiseurl(v):
	t = list(set(v['URL']))
	vv = []
	for x in t:
        	if not excludesearch(x):
                	vv.append(x)
	return vv

def calculateWordFreq(urltoworddict):
	wordlist = []
	for value in urltoworddict.values():
		wordlist.extend(value)
	wordlistfreq = Counter(wordlist)
	return wordlistfreq

		

if __name__ == "__main__":
	v = pd.read_csv('/home/account1/Gettysegmentation/GettyUKURL.txt', '\t')

	tt = removenoiseurl(v)
	urltoworddict = geturltoworddict(tt)
	with open('GettyUKUrlOutput.csv','w') as f:
		for k,v in urltoworddict.iteritems():
			#Sorting the counters based on the phrases
			t = sorted(v.items(), key =lambda x: len(x[0]))
			totalfreq = sum(elements[1] for elements in t)
			f.write(k+'\t'+str(totalfreq)+'\n')
			tt = [str(x[0])+'\t'+str(x[1]) for x in t]
			f.write('\n'.join(tt)+'\n\n')
	print 'arnab'
