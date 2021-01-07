#Making a health calculator with 3 inputes
def health_calculator(age, apples, cigarettes):
  answer = (age + 100) + (apples * 3.5) - (cigarettes * 2)
  print(answer)
#Roee, along with many other users, have all this data compiled
roee_data = [19, 10, 0]
#Instead of writing each item, use * to "unpack" data list, and saves time and coding
health_calculator(roee_data[0], roee_data[1], roee_data[2])
health_calculator(*roee_data)
#Will give the same answer as writing it all in manually