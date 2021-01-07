#If want to loop through anything, use range (Will go through 0-9)
for x in range(10):
    print(x)
    
#Give it a defined limit
for x in range(5, 12):
    print(x)
    
#Give it an increment, 10-40 with increments of 5
for x in range(10, 40, 5):
    print(x)

#While will loop as long as condition is true
#5 is always less than 10, so it will infinite loop    
z = 5
while z < 10:
    print(z)
    z += 1