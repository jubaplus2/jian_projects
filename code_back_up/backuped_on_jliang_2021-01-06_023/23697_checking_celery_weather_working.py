
import datetime
import os
import glob
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
import logging

logging.basicConfig(filename='log_of_checking_celery_working_status.log', level=logging.INFO)
logging.info(str(datetime.datetime.now()) +": checker started")
sender="jubapluscc@gmail.com"
receivers='jian@jubaplus.com' # To be motified if multiple receivers using ", ".join
# subject="Pacing Reports"
today_str=str(datetime.datetime.now().date())

BL_daily_weather_file_list = glob.glob("/home/sharefolder/Weather/Json_data/daily/"+today_str+"*.json")
BL_forecast_weather_file_list = glob.glob("/home/sharefolder/Weather/Json_data/forecast/"+today_str+"*.json")

# SK_daily_weather_file_list = glob.glob("/home/jian/Projects/Smoothie_King/Weather_Data/Daily_Actual/"+today_str+"*.json")
# SK_forecast_weather_file_list = glob.glob("/home/jian/Projects/Smoothie_King/Weather_Data/Forecast/"+today_str+"*.json")


if (len(BL_daily_weather_file_list)==3) & (len(BL_forecast_weather_file_list)==3)):
	logging.info(today_str+": working all right."+ str(datetime.datetime.now()))
	print(today_str+": working all right."+ str(datetime.datetime.now()))
else:
	logging.info(today_str+": working wrong." + str(datetime.datetime.now()))
	print(today_str+": working wrong." + str(datetime.datetime.now()))
	subject="Jian_Checking_192_celery_working_status"
	msg = MIMEMultipart()
	msg['From'] = sender
	msg['To'] = receivers
	msg['Date'] = formatdate(localtime=True)
	msg['Subject'] = subject
	with open('/home/jian/celery_tracker_working/email_content_celery_tracker.txt','r') as f:
		text_mesaage = f.read()
	msg.attach(MIMEText(text_mesaage))


	smtp = smtplib.SMTP('smtp.gmail.com',587)
	smtp.ehlo()
	smtp.starttls()
	smtp.login(sender,"mfppxsfikqmazbqj")
	smtp.send_message(msg)
	logging.info(today_str+": celecry worker is down")
	smtp.close()
	


