import os

def getAllfiles(path):
        #for file in os.listdir("/disk2/Projects/Rentrak/newData/20160808-20160821"):
        fileList = []
        for file in os.listdir(path):
                if file.endswith(".csv"):
                        fileList.append(file)
        return fileList


path = '/disk2/arnab/newrentrak/output'
x = getAllfiles(path)
path = '/disk2/arnab/newrentrak/res-20160725-20160807/output'
path = '/disk2/arnab/newrentrak/res-20160711-20160724/output'
y = getAllfiles(path)
print x
print y
