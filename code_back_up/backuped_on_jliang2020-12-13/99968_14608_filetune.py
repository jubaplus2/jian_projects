import pandas as pd
v = pd.read_csv('other.txt','\t')
t = v.iloc[0:,0]
t.to_csv('ll.csv',index=False)