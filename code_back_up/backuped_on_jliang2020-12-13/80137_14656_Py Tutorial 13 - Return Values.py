#Return values stores response of functions to use it later in program
#We'll make a function of appropriate age for girls to date
def allowed_dating_age(my_age):
    girls_age = (my_age / 2) + 7
    return girls_age
    
#The return value is stored now, so just running the function yields nothing
#Utilize the return value of function by doing a print function
Roee_limit = allowed_dating_age(19)
print("Roee can date girls", Roee_limit, "or older")
Yoram_limit = allowed_dating_age(50)
print("Yoram can date girls", Yoram_limit, "or older")