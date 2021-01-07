# -*- coding: utf-8 -*-
# File Name: main.py
# Author: Ben Ho
# Editor: Ben Ho
# Created: 2020/06/14
# Last Edited: 2020/06/14
# Description: Execution for Building Dataset of POS Predictions



import sys
sys.path.append("/home/jian/Projects/Big_Lots/Predictive_Model/extract_from_MySQL/Ben/")  # like sys.path.append("../tests")

from etl_pos_all_JL_0809 import EtlPos
from etl_process_part_2 import write_crm_train_test
from etl_process_part_2 import get_dist_output_df
from BLLR_model_building import run_predicting


if __name__ == "__main__":
    
    # etl = EtlPos()
    # etl.run()
    # write_crm_train_test()
    run_predicting()
    

