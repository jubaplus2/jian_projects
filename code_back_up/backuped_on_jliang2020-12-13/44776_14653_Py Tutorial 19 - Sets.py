#Set is a collection of items, but no duplicates unlike lists, use {}
groceries = {'cereal', 'milk', 'chocolate', 'beer', 'soap', 'beer'}
#When you print it, it will only print beer once
print(groceries)

if 'milk' in groceries:
    print("Already have mik")
else:
    print("Need milk")