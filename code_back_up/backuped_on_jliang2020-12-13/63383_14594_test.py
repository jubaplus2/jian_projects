import enchant
def checkValidWords(input):
	wordDict = enchant.Dict("en_US")
	return True if input and wordDict.check(input) else False

print checkValidWords('cu')
