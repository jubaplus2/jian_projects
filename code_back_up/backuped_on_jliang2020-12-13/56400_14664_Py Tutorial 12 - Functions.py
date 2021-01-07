#Function breaks up program into manageable chunks to better organize
def x():
    print("Hello")
    
#Even though made a function, we have to call the function to use it
x()

#Inside the parantheses, put extra info, like the amount of bitcoin converted
def bitcoin_to_USD(btc):
    amount = btc * 527
    print(amount)
#We can put 5 in parantheses, to calculate 5 bitcoins to USD amount
bitcoin_to_USD(5)
bitcoin_to_USD(10)