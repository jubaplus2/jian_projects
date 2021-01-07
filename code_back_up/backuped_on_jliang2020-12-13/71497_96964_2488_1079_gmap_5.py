import pandas as pd
import numpy as np
import google_analytics_reporting_api as ga
import json as json
import pandas as pd
import numpy as np
import googlemaps



Lat_Long_to_Zip=pd.read_csv("/home/jian/Projects/Saatva/GA/lat_long_list.csv")

gmap_1=Lat_Long_to_Zip.iloc[0:2000,:]
gmap_2=Lat_Long_to_Zip.iloc[2000:4000,:]
gmap_3=Lat_Long_to_Zip.iloc[4000:6000,:]
gmap_4=Lat_Long_to_Zip.iloc[6000:8000,:]
gmap_5=Lat_Long_to_Zip.iloc[8000:10000,:]
gmap_6=Lat_Long_to_Zip.iloc[10000:12000,:]
gmap_7=Lat_Long_to_Zip.iloc[12000:14000,:]
gmap_8=Lat_Long_to_Zip.iloc[14000:16000,:]
gmap_9=Lat_Long_to_Zip.iloc[16000:,:]
gmap_1.reset_index(inplace=True)
gmap_2.reset_index(inplace=True)
gmap_3.reset_index(inplace=True)
gmap_4.reset_index(inplace=True)
gmap_5.reset_index(inplace=True)
gmap_6.reset_index(inplace=True)
gmap_7.reset_index(inplace=True)
gmap_8.reset_index(inplace=True)
gmap_9.reset_index(inplace=True)

del gmap_1['index']
del gmap_2['index']
del gmap_3['index']
del gmap_4['index']
del gmap_5['index']
del gmap_6['index']
del gmap_7['index']
del gmap_8['index']
del gmap_9['index']

key1='AIzaSyCcno2dFME8A7vZta-TjB5kyYhGIcPra40'
key2='AIzaSyCX271igKGwh5xZJIDSHZuh6Lkhn2rTKDQ'
key3='AIzaSyBWPSucpI1SsDR3ry6LeEJpVfHx93v16Zs'
key4='AIzaSyDCuN_k1DabYyEN71-L-ZDkE20RNVP5_fw'
key5='AIzaSyDnOK3kuDIG0zVZam-dM6Dg4CSUw7A-Jqk'
key6='AIzaSyDxfeF53iuJFhOd9bP09Nda0yOFcvlBttk'
key7='AIzaSyAca5Rgb87BtzqEZ9taQFRSfA3aDWx9UeU'
key8='AIzaSyDwZ5lebWYsUILyA0xE9WpJV2JQwZPrAaM'
key9='AIzaSyDPQS_84g5KP9fTcGLkRrGou_ngteSWpfs'
key10='AIzaSyA9Db16E75O9AIFhUnUwWSCc6FGjJVSqxE'
key11='AIzaSyBR-wCK15ScnmLeXtbT6gHfAKxIBm1hUXs'

gmaps_1_key = googlemaps.Client(key=key1)
gmaps_2_key = googlemaps.Client(key=key2)
gmaps_3_key = googlemaps.Client(key=key3)
gmaps_4_key = googlemaps.Client(key=key4)
gmaps_5_key = googlemaps.Client(key=key5)
gmaps_6_key = googlemaps.Client(key=key6)
gmaps_7_key = googlemaps.Client(key=key7)
gmaps_8_key = googlemaps.Client(key=key8)
gmaps_9_key = googlemaps.Client(key=key9)
gmaps_10_key = googlemaps.Client(key=key10)
gmaps_11_key = googlemaps.Client(key=key11)

wrtier="/home/jian/Projects/Saatva/GA/gmap_5.csv"
for i in range(len(gmap_5)):
    lat = gmap_5.iloc[i,0]
    long = gmap_5.iloc[i,1]
    reverse_geocode_result = gmaps_5_key.reverse_geocode((lat, long))
    address_components_list=reverse_geocode_result[0]['address_components']
    for j in range(len(address_components_list)):
        if address_components_list[j]['types']==['country', 'political']:
            gmap_5['Country'][i]=address_components_list[j]['short_name']
        if address_components_list[j]['types']==['postal_code']:
            gmap_5['zip_code'][i]=address_components_list[j]['short_name']
            
gmap_5.to_csv(wrtier,index=False)
            
            
            
            
            
            
            
            
    