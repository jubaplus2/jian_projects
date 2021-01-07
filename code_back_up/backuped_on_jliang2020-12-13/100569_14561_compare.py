import pandas as pd

def func(v1):
	c = 0
	x1 = set()
	while c <len(v1):
		x = list(v1.iloc[c,1:4])
		x = [str(t) for t in x]
		x1.add(''.join(x))
		c+=1
	return x1

v1 = pd.read_csv('1925577.txt','\t')
v2 = pd.read_csv('2366514.txt','\t')
x1 = func(v1)
x2 = func(v2)
print x1 - x2
print x2 - x1
