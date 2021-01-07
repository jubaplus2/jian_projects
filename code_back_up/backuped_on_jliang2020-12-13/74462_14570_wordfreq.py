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
#gettyimages so should we exclude images
#istockphoto
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



def geturltoworddict(vv):
	urltoworddict = {}
	for x in vv:
		urltoworddict[x] = validWord(getWordsFromURL(x))
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
	wordlistfreq = calculateWordFreq(urltoworddict)
	with open('GettyUKUrlOutput.csv','w') as f:
		f.write('URL\tWords\n')
		for k,v in urltoworddict.iteritems():
			result = [elements+'('+str(wordlistfreq[elements])+')'for elements in v]
			#f.write(k.replace(',','')+'\t'+'\t'.join(v)+'\n')
			f.write(k.replace(',','')+'\t'+'\t'.join(result)+'\n')
	print 'arnab'
