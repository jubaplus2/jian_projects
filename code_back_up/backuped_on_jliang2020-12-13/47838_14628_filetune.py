import pandas as pd
v = pd.read_csv('music.txt','\t')
t = v.iloc[0:,0]
t.to_csv('ll.csv',index=False)
