import pandas as pd
def previousweekFile(inputfile):
        v = pd.read_csv(inputfile)
        t = list(v.keys())
        #Appending the new week rank information
        #Below dict contains keys as ShowID\tNetwork_no\tShowName\tUSNG_100\tZip\tZip4
        #and values as list of ranks across weeks
        weekinfodict = {}
	lenv = len(v)
	counter = 0
	while counter < lenv:
                xx = v[t[0]][counter].split('\t')
                weekinfodict['\t'.join(xx[0:-1])] = xx[-1]
		counter+=1
                
        t.append('Rank'+path.split('/')[-2])
        #Note t is going to be the new header of the new output file
        return t, weekinfodict

previousweekFile('outputWeek1.csv')
