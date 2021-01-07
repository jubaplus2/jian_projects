import pandas as pd

data = pd.read_csv('out1.csv', index_col = 0)
data2 = pd.read_csv('input.csv', skiprows = 1, header = [0, 1], index_col = 1)

print len(list(data.columns.values))
# print list(data2.columns.values)
for col in list(data2.columns.values)[1:]:
    if 'Sig.' in col:
        Sig = col
    if 'VIF' in col:
        VIF = col
for idx, col in enumerate(list(data.columns.values)):
    if idx >= 3: 
        if data2[Sig][idx - 2] > 0.1 \
               or data2[VIF][idx - 2] > 5:
            data.drop(col, axis = 1, inplace = True)
            continue
        else:        
            rows = data[col].count()
            counts = data[col].value_counts()
            if 0 in counts:
                if counts[0] * 1.0 / rows > 0.95:
                    data.drop(col, axis = 1, inplace = True)

data.to_csv('out.csv')
print len(list(data.columns.values))
