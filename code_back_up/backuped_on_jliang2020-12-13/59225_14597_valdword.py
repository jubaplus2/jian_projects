import nltk
from nltk.corpus import words
validwordlist = words.words()
pstemmer = nltk.PorterStemmer()
lstemmer = nltk.LancasterStemmer()
wnl = nltk.WordNetLemmatizer()



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


print checkValidWords('pho')
