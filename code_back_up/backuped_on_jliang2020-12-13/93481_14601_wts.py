import pandas as pd
v = pd.read_csv('gettyusphotos.txt',sep = '\t')
v['wt'] = 1
v.to_csv('analysis.txt',sep='\t',index=False)
