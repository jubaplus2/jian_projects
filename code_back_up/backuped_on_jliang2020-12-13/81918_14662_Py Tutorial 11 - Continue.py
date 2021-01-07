numberstaken = [2, 8, 11, 15, 17]

#Continue is like break, but keeps going through loop after skips over n in list
for n in range(1, 21):
    if n in numberstaken:
        continue
    print(n)