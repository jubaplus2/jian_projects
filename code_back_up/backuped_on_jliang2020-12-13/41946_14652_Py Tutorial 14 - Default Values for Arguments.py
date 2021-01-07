#Ex: social network asks for sex, if they don't pick M or F need default value
def gender(sex = 'Unknown'):
    if sex is 'M':
        sex = 'Male'
    elif sex is 'F':
        sex = 'Female'
    print(sex)
    
#Now we test for each scenario, and each will return unique if statement
    #the empty parantheses will return the default value already set
gender('M')
gender('F')
gender()