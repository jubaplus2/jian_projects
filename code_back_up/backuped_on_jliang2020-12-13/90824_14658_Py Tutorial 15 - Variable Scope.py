a = 1738
#Variable defined must be above functions in order to access them
def x():
    a = 1738
    print()
    
#Won't print it out if variable defined in another function
def y():
    print(a)
    
x()
y()