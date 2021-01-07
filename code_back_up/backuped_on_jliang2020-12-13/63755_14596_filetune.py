import pandas as pd
v = pd.read_csv('/disk2/Projects/Gettyurlanalysis/wordremoved/GettyUSURLOutputVideo.txt','\t')
t = v.iloc[0:,0]
t.to_csv('ll.csv',index=False)
