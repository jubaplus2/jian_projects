#{'key': 'US:16124'} value should be like this
def getZipDetails(inputfileName):
        with open(inputfileName) as t:
                g = t.read().split('\n')
                g.remove('')
        finallist = ['US:'+elements for elements in g]
        ffinallist = [{"key": elements} for elements in finallist]
        return ffinallist

#print getZipDetails('../config/szip.csv')

def getZip(input):
	return {'key': 'US:'+input}