from urllib import request

#Downloading .csv, set variable to the .csv
google_stock_price = 'http://real-chart.finance.yahoo.com/table.csv?s=GOOG&d=5&e=29&f=2016&g=d&a=7&b=19&c=2004&ignore=.csv'

#Create a function, with the input being the url
#Open the file with request.urlopen function, read with response.read
    #make the all the data into string, organize it in lines, and save it
        #to a named file name googstock.csv, open the newly created file
            #give it a write function and loop the line + a new line ("\n")
                #and close it
def download_stock_data(csv_url):
    response = request.urlopen(csv_url)
    csv = response.read()
    csv_str = str(csv)
    lines = csv_str.split("\\n")
    dest_url = r"googstock.csv"
    fx = open(dest_url, "w")
    for line in lines:
        fx.write(line + "\n")
    fx.close

#Call the function, and should open an excel file with the stock information
    #called googstock.csv
download_stock_data(google_stock_price)
