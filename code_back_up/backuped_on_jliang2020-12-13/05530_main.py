# -*- coding: utf-8 -*-
# File Name: main.py
# Author: Ben Ho
# Editor: Ben Ho
# Created: 2020/06/14
# Last Edited: 2020/06/14
# Description: Execution for Building Dataset of POS Predictions



import sys
sys.path.append("/home/jian/Projects/Big_Lots/Predictive_Model/extract_from_MySQL/Ben/")  # like sys.path.append("../tests")

from etl_pos import EtlPos


if __name__ == "__main__":
    etl = EtlPos()
    etl.run()

