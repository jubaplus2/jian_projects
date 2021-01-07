import os
import commands
#print commands.getstatusoutput('wc -l')

path = '/disk2/arnab/newrentrak/output'
'''

cmd1 = 'mysql NewRentrakDB -e "create table Show1854638 (ShowID varchar(40), Network varchar(40), ShowName varchar(40), USNG_100 varchar(40), Week varchar(40), Decile varchar(40))"';
cmd2 = 'mysqlimport --ignore-lines=1 --fields-terminated-by='\t' --verbose --local NewRentrakDB /disk2/arnab/newrentrak/output/Show1854638.csv'
'''

def getAllfiles(path):
        #for file in os.listdir("/disk2/Projects/Rentrak/newData/20160808-20160821"):
        fileList = []
        for file in os.listdir(path):
                if file.endswith(".csv"):
                        fileList.append(file)
        return fileList


if __name__ == "__main__":
	fileList = getAllfiles(path)
	for elements in fileList:
		try:
			result = 'mysql NewRentrakDB -e '+'\"create table '+ elements.split('.csv')[0] +' (ShowID varchar(40), Network varchar(40), ShowName varchar(40), USNG_100 varchar(40), Week varchar(40), Decile varchar(40))\"'
			commands.getstatusoutput(result)
		except:
			print 'caught exception'
			pass
		result = 'mysqlimport --ignore-lines=1 --fields-terminated-by=\'\\t\' --verbose --local NewRentrakDB '+ path+'/'+elements
		print result
		commands.getstatusoutput(result)
		print elements
	print 'arnab'
		
