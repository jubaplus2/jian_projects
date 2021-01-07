foods = ['bacon', 'beer', 'chicken', 'salad']
foods

#Want program to print all items out, without having to repeat print() function
#For will loop through, with a variable representing each item
for f in foods:
    print(f)
    print(len(f))

#Will just loop through position 1 and 2 (stop point is 3, not inclusive)
for f in foods[1 : 3]:
    print(f)
    print(len(f))