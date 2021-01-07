# -*- coding: utf-8 -*-
# File Name: main_run_predictive_model.py
# Author: Jian
# Editor: Jian
# Created: 2020/12/31
# Last Edited: 2020/12/31
# Description: The driver to run predictive model

from update_high_date_in_config import update_high_date_in_config
update_high_date_in_config()


import logging 
import datetime
logging.basicConfig(filename='./log_main_run_predictive_model_%s.log'%str(datetime.datetime.now().date()),level=logging.INFO)
import sys
import json
config_in_main=json.load(open("./config.json","r"))
script_folder=config_in_main['script_folder']
sys.path.append(script_folder)  # like sys.path.append("../tests")


from BL_weekly_cron01_Tue_per_week import copy_files_and_update_MySQL
from etl_pos_all_JL_0809 import EtlPos
from etl_process_part_2_updated_1226 import write_crm_train_test
from etl_process_part_2_updated_1226 import get_dist_output_df
# from BLLR_model_building import run_predicting
from predict_with_current_week_data import predict_current_week
from delete_old_tables import remove_tables_4_weeks_ago
from Audience_Weekly_uploading import update_audience

start_time=datetime.datetime.now()
logging.info("Job start: %s"%str(start_time))
if __name__ == "__main__":
    #0 update MySQL
    logging.info("Start copy_files_and_update_MySQL: %s"%str(datetime.datetime.now()))
    copy_files_and_update_MySQL()
    logging.info("Done copy_files_and_update_MySQL: %s"%str(datetime.datetime.now()))

	#1 
    logging.info("Start ETLPos: %s"%str(datetime.datetime.now()))
    etl = EtlPos()
    etl.run()
    logging.info("Done ETLPos: %s"%str(datetime.datetime.now()))

    #2
    logging.info("Start write_crm_train_test: %s"%str(datetime.datetime.now()))
    write_crm_train_test()
    logging.info("Done write_crm_train_test: %s"%str(datetime.datetime.now()))
    
    #run_predicting() #old version to run the validation only

    #3
    logging.info("Start predict_current_week: %s"%str(datetime.datetime.now()))
    predict_current_week()
    logging.info("Done predict_current_week: %s"%str(datetime.datetime.now()))

    #4
    logging.info("Start remove_tables_4_weeks_ago: %s"%str(datetime.datetime.now()))
    remove_tables_4_weeks_ago()
    logging.info("Done remove_tables_4_weeks_ago: %s"%str(datetime.datetime.now()))

    # 5
    logging.info("Start update_audience: %s"%str(datetime.datetime.now()))
    update_audience()
    logging.info("Done update_audience: %s"%str(datetime.datetime.now()))

end_time=datetime.datetime.now()
logging.info("Job finished: %s"%str(end_time))
logging.info("Whole running time: %s"%str(end_time-start_time))
