#Open function opens a file, takes two parameters, name of file and the action
fw = open("sample.txt", "w")
#\n allows to continue to write text on new line
fw.write("Writing some stuff in text file\n")
fw.write("I like beer\n")
#fw.close closes the file
fw.close()
#Running program opens file with the text you wrote

#Stored the text we want to open in a variable we named "text"
fr = open("sample.txt", "r")
text = fr.read()
print(text) #will display the text
fr.close