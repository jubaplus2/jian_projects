from db import *

#showdetails = 'KendraonTop2150186'
showdetails = 'Love&HipHopHollywood2293230.csv'
rank = 'Rank20150907-20150920'

users = createTable(showdetails, 'ShowID', 'Network_no', 'ShowName', 'USNG_100', 'Zip', 'Zip4', rank)
