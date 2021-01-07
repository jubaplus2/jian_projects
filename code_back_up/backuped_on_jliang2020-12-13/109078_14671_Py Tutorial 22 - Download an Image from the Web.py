#Import random again, urllib.request allows us to access data from web
import random
import urllib.request

#Only parameter is a url from the web
#Want to name image random number, hence the random.randrange function
#Full name will be the name, which is a number, but must be converted into a string
    #so use the str() function + ".jpg" to store it as image
#Reference urllib.request, then . the function of retrieving a url, and naming it
def download_web_image(url):
    name = random.randrange(1, 101)
    full_name = str(name) + ".jpg"
    urllib.request.urlretrieve(url, full_name)
    
#Call function, and copy paste any web image url into parantheses as a string  
download_web_image("http://www.pace.edu/sites/default/files/styles/t20_related_story/public/related-story-media-storm_0.gif?itok=wDn0rEVn")
#Should download and save into your computer as .jpg