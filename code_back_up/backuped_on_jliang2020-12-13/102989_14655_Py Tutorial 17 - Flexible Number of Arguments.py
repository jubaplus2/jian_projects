#Function with any amount of arguments, use * and any variable (args)
def add_numbers(*args):
    total = 0
    for a in args:
        total += a
    print(total)
    
#Now multiple arguments added together
add_numbers(3, 32, 3452, 123, 5312)