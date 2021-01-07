#In Python, a dictionary defined keys and values
classmates = {'Spencer' : 'Good programmer', 'Roee' : "Yoram's son", 'Joann' : 'Smart'}

#If you put a key into [], will return it's value
print(classmates['Spencer']) #returns only "Good programmer"

#Can loop through each key and value
for k, v in classmates.items():
    print(k +  v)