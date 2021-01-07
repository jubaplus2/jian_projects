import itertools

def getFinalCombinatlist(permdictKeys, conditiondictKeys, locationKeys):
	permList = []
	for x in range(1,len(permdictKeys)+1):
        	permList.extend(list(itertools.combinations(permdictKeys, x)))
	finaList = [permList, conditiondictKeys, locationKeys]
	combList = list(itertools.product(*finaList))
	print len(combList)
	return combList
