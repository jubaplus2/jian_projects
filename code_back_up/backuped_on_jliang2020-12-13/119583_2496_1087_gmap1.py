# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import googlemaps
import pandas as pd
import numpy as np
LatLong=pd.read_excel("/home/jian/Projects/Saatva/For_Joann_Gmap/latlong-1.xls")
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

set_1=LatLong.iloc[0:2000,]
set_1.reset_index(inplace=True)
del set_1['index']
set_2=LatLong.iloc[2000:,]
set_2.reset_index(inplace=True)
del set_2['index']


def zip_match(df,key_str):
    key=googlemaps.Client(key=key_str)
    df['Country']='nan'
    df['zip_code']='nan'
    for i in range(len(df)):
        lat = df.iloc[i,0]
        long = df.iloc[i,1]
        reverse_geocode_result = key.reverse_geocode((lat, long))
        
        for j in range(len(reverse_geocode_result)):
            for k in range(len(reverse_geocode_result[j]['address_components'])):
                if reverse_geocode_result[j]['address_components'][k]['types']==['country', 'political']:
                    df['Country'][i]=reverse_geocode_result[j]['address_components'][k]['short_name']
                    print([i,j,k])
                    break
                else:
                    continue
            if df['Country'][i]!='nan':
                break
            else:
                continue
 

        for l in range(len(reverse_geocode_result)):
            for k in range(len(reverse_geocode_result[l]['address_components'])):
                if reverse_geocode_result[l]['address_components'][k]['types']==['postal_code']:
                    df['zip_code'][i]=reverse_geocode_result[l]['address_components'][k]['short_name']
                    print([i,l,k])
                    break
                else:
                    continue    
            if df['zip_code'][i]!='nan':
                break
            else:
                continue

    return df



set_1_output=zip_match(set_1,key3)
set_1_output.to_csv("/home/jian/Projects/Saatva/For_Joann_Gmap/Lat_long_zip_1.csv",index=False)

