import pandas as pd
import numpy as np
v = pd.read_csv('/disk2/Projects/Rentrak/Mediastorm_USNG_tile_query_updated_with_geography.txt')
columns = ['zip','zip4','lat','long','USNG_tile']
df = pd.DataFrame(v,columns=columns)
df = df.replace(np.nan,"0000", regex=True)
df.to_csv('res.csv',index = False)
