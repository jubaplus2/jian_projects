magicnumber = 25
#Loop through 1-100 to find magic number
#Break statement stops the loop, so once the loop gets to 25 it will stop
for n in range(101):
    if n is magicnumber:
        print(n, "is the magic number")
        break
    else:
        print(n)
    