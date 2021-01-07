# -*- coding: utf-8 -*-
# File Name: etl_pos.py
# Author: Ben Ho
# Editor: Ben Ho
# Created: 2020/06/13
# Last Edited: 2020/07/03

# Updated by Jian: 2020-07-13
# rencet 2 transactions only

import sqlalchemy as sql
import json
import pandas as pd
import datetime
import os
from number_to_word import NumberToWords
from functools import reduce
import numpy as np
from haversine import haversine
import glob

class EtlPos:
    """Generate Dataset of POS Prediction by ETL MySQL Databases

    To use:
     etl = EtlPos()
     etl.run()
     FINISH !!
    """

    def __init__(self):
        # Configure
        self.username = ""
        self.password = ""
        self.database = ""
        self.pos_table = ""
        self.crm_table = ""
        self.exposure_table = ""
        self.activity_table = ""
        self.pos_start_date = ""
        self.pos_start_date_id_filter = ""
        self.recent_n_month=""
        self.pos_end_date = ""
        self.crm_start_date = ""
        self.crm_end_date = ""
        self.is_with_dcm = False
        self.sample_ratio = 1
        self.database_update_period = []
        # MySQL
        self.engine = None
        self.department_ids = list()
        self.department_prefix = list()
        self.table_dcm_id = ""
        self.table_id_from_crm = ""
        self.table_filtered_id = ""
        self.table_filtered_pos = ""
        self.table_filtered_crm = ""
        self.table_max_trans_order = ""
        self.table_2_1 = ""
        self.table_2_2 = ""
        self.table_crm_id_list_train = ""
        self.table_crm_id_list_test = ""
        self.table_1 = ""
        
        # MySQL validator
        self.record = {}
        # Output
        self.columns_table_2_1 = list()
        self.columns_table_2_2 = list()
        self.map_columns_table_2_2 = dict()
        self.base_directory = ""
        self.csv_table_2_1 = ""
        self.csv_table_2_2 = ""
        self.folder_store_list=""
        self.folder_email_unsub=""
        self.path_TA_excel=""
        self.path_json_zip_center=""


    def run(self):
        """Driver function.

        Args:
            None.

        Return:
            None.

        Raises:
            MySQL ConnectionError.
        """
        print("Start!!")
        start = datetime.datetime.now()
        self.configure()
        self.check_database_updating()
        self.connect_to_mysql()
        self.preprocess()
        self.retrieve_department_ids()
        self.add_prefix_of_department()
        self.build_table_2_1()
        self.build_table_2_2()
        self.build_table_1_with_split()
        # self.output_to_csv()
        end = datetime.datetime.now()
        print("!!!DONE!!!")
        self.print_time_consuming(task="Complete Whole Process", start=start, end=end)

    def configure(self):
        """Configuration.

        Args:
            None.

        Return:
            None.

        Raises:
            None
        """
        start = datetime.datetime.now()
        with open('./config.json', 'rb') as f:
            data = json.load(f)
        self.username = data["username"]
        self.password = data["password"]
        self.database = data["database"]
        self.pos_table = data["pos_table"]
        self.crm_table = data["crm_table"]
        self.exposure_table = data["exposure_table"]
        self.activity_table = data["activity_table"]
        self.pos_start_date = data["pos_start_date"]
        self.pos_end_date = data["pos_end_date"]
        self.recent_n_month=data['recent_n_month']
        if self.recent_n_month:
            self.pos_start_date_id_filter = str(pd.to_datetime(self.pos_end_date).date()-datetime.timedelta(days=int(np.ceil(365*self.recent_n_month/12))))
        else:
            self.pos_start_date_id_filter = data["pos_start_date"]

        self.crm_start_date = data["crm_start_date"]
        self.crm_end_date = data["crm_end_date"]
        self.is_with_dcm = data["is_with_dcm"]
        self.sample_ratio = data["sample_ratio"]
        self.random_size=data['random_size']
        self.database_update_period = data["database_update_period"]
        self.base_directory = data["base_directory"]
        if self.base_directory[-1] != '/':
            self.base_directory += '/'
        end = datetime.datetime.now()
        self.folder_store_list=data['folder_store_list']
        self.folder_email_unsub=data['folder_email_unsub']
        self.path_TA_excel=data['path_TA_excel']
        self.path_json_zip_center=data['path_json_zip_center']


        self.print_time_consuming(task="Configuration", start=start, end=end)
        return

    def check_database_updating(self):
        """Check if the database is updating in case of locking.

        Args:
            None.

        Return:
            None.

        Raises:
            The database xxx Updates Every [n in Monday to Sunday]
        """
        weekdays = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
        today_weekday = weekdays[datetime.datetime.now().weekday()]
        if today_weekday in self.database_update_period:
            period = ', '.join(self.database_update_period)
            raise ValueError(
                "The Database %s Updates Every %s. Please avoid these days" % (self.database, period))

    def connect_to_mysql(self):
        """Connect to MySQL.

        Args:
            None.

        Return:
            None.

        Raises:
            MySQL Connection Error.
        """
        start = datetime.datetime.now()
        self.engine = sql.create_engine(
            "mysql+pymysql://%s:%s@localhost/%s" % (self.username, self.password, self.database))
        end = datetime.datetime.now()
        self.print_time_consuming(
            task="Connect to MySQL Database %s with username: %s ; password: %s" % (
                self.database, self.username, self.password), start=start,
            end=end)
        return

    def preprocess(self):
        def choose_and_filter_id():
            if self.is_with_dcm:
                generate_filtered_id_from_dcm()
            else:
                choose_id_from_crm()
                filter_id_from_pos()

        def generate_filtered_id_from_dcm():
            self.crm_end_date = str(
                datetime.datetime.strptime(self.pos_end_date, "%Y-%m-%d").date() - datetime.timedelta(days=7 * 12))
            crm_id_query = "select distinct customer_id_hashed as customer_id_hashed from %s crm where %s;" % (
                self.crm_table, append_date_range_condition("crm.sign_up_date", start_date=self.crm_start_date,
                                                            end_date=self.crm_end_date))
            pos_id_query = "select distinct customer_id_hashed as customer_id_hashed from %s pos where %s;" % (
                self.pos_table,
                append_date_range_condition("pos.transaction_dt", start_date="", end_date=self.pos_end_date))
            exposure_id_query = "select distinct customer_id_hashed as customer_id_hashed from %s exp where %s;" % (
                self.exposure_table,
                append_date_range_condition("exp.date_est", start_date="", end_date=self.pos_end_date))
            df_list = [self.read_sql_to_pandas_with_timer(query=crm_id_query),
                       self.read_sql_to_pandas_with_timer(query=pos_id_query),
                       self.read_sql_to_pandas_with_timer(query=exposure_id_query)]
            pd_start = datetime.datetime.now()
            df_common_id = reduce(lambda left, right: pd.merge(left, right, on=["customer_id_hashed"], how="inner"),
                                  df_list)
            del df_list
            print("Shape of DCM common id", df_common_id.shape)
            print("Number of Unique DCM id", df_common_id['customer_id_hashed'].nunique())
            pd_end = datetime.datetime.now()
            self.print_time_consuming(task="Common ID from Merging DCM Tables", start=pd_start, end=pd_end)
            self.table_filtered_id = "Pred_Temp_CommonID_ending_%s_since18Q1" % self.pos_end_date.replace("-", "")
            dtype = {"customer_id_hashed": sql.types.VARCHAR(length=64)}
            self.write_pandas_to_sql_with_timer(dataframe=df_common_id, table_name=self.table_filtered_id, dtype=dtype)
            self.create_index(table_name=self.table_filtered_id, list_of_columns=["customer_id_hashed"])
            query_unique_id = "SELECT DISTINCT(customer_id_hashed) FROM %s;" % self.table_filtered_id
            uniqueness = self.execute_with_timer(query=query_unique_id, task="Count Uniqueness of IDs")
            self.record[self.table_filtered_id] = uniqueness[1]
            return

        def choose_id_from_crm():
            self.table_id_from_crm = self.generate_table_name(source_table_name="crm", is_from_source=True)
            clause_create_table = "CREATE TABLE %s AS " % self.table_id_from_crm
            clause_select = "SELECT DISTINCT(crm.customer_id_hashed) FROM %s crm " % self.crm_table
            if self.random_size:
                clause_where = "WHERE %s ORDER BY RAND() limit %s;" %(
                    append_date_range_condition(
                        "crm.sign_up_date", start_date=self.crm_start_date, end_date=self.crm_end_date,previous_condition=False),
                    str(self.random_size)
                    )
            elif self.sample_ratio<1:
                clause_where = "WHERE RAND() < %f %s;" % (
                 self.sample_ratio, append_date_range_condition(
                     "crm.sign_up_date", start_date=self.crm_start_date, end_date=self.crm_end_date,
                     previous_condition=True))
            else:
                clause_where = "WHERE %s;" %append_date_range_condition(
                "crm.sign_up_date", start_date=self.crm_start_date, end_date=self.crm_end_date,
                previous_condition=False)

            list_of_clause = [clause_create_table, clause_select, clause_where]
            query = self.concat_clause(list_of_clause=list_of_clause)
            self.execute_with_timer(query=query, task="SELECT ID from CRM", table_name=self.table_id_from_crm,
                                    list_of_columns=["customer_id_hashed"])
            return

        def filter_id_from_pos():
            self.table_filtered_id = self.generate_table_name()
            self.execute_dropping_table_with_timer(task="DROP TABLE of Filtered ID IF EXISTS",
                                                   table_name=self.table_filtered_id)
            clause_create_table = "CREATE TABLE %s AS " % self.table_filtered_id
            clause_select = "SELECT DISTINCT(ids.customer_id_hashed) FROM %s ids INNER JOIN %s pos ON \
                             ids.customer_id_hashed = pos.customer_id_hashed " % (
                self.table_id_from_crm, self.pos_table)
            clause_where = "WHERE %s;" % append_date_range_condition("pos.transaction_dt",
                                                                     start_date=self.pos_start_date_id_filter,
                                                                     end_date=self.pos_end_date)
            list_of_clause = [clause_create_table + clause_select + clause_where]
            query = self.concat_clause(list_of_clause=list_of_clause)
            self.execute_with_timer(query=query, task="Filter ID from POS", table_name=self.table_filtered_id,
                                    list_of_columns=["customer_id_hashed"])
            query_unique_id = "SELECT DISTINCT(customer_id_hashed) FROM %s;" % self.table_filtered_id
            uniqueness = self.execute_with_timer(query=query_unique_id, task="Count Uniqueness of IDs")
            self.record[self.table_filtered_id] = uniqueness[1]
            return

        def filter_crm():
            self.table_filtered_crm = self.generate_table_name(prefix="crm")
            self.execute_dropping_table_with_timer(task="DROP TABLE of Filtered CRM IF EXISTS",
                                                   table_name=self.table_filtered_crm)
            clause_create_table = "CREATE TABLE %s AS " % self.table_filtered_crm
            clause_select = "SELECT crm.customer_id_hashed, MIN(crm.sign_up_date) AS sign_up_date FROM %s common \
        INNER JOIN %s crm ON common.customer_id_hashed = crm.customer_id_hashed GROUP BY crm.customer_id_hashed;" % \
                            (self.table_filtered_id, self.crm_table)
            list_of_clause = [clause_create_table, clause_select]
            query = self.concat_clause(list_of_clause=list_of_clause)
            list_of_columns = ["customer_id_hashed", "sign_up_date"]
            result = self.execute_with_timer(query=query, task="Filter CRM", table_name=self.table_filtered_crm,
                                             list_of_columns=list_of_columns)
            self.record[self.table_filtered_crm] = result[1]
            return

        def filter_pos():
            self.table_filtered_pos = self.generate_table_name(prefix="pred_pos_dept")
            self.execute_dropping_table_with_timer(task="DROP TABLE of Filtered POS IF EXISTS",
                                                   table_name=self.table_filtered_pos)
            clause_create_table = "CREATE TABLE %s AS " % self.table_filtered_pos
            clause_select = "SELECT pos.customer_id_hashed, pos.transaction_dt, pos.trans_order_since_18Q1, pos.units, \
        pos.sales, pos.location_id, pos.department_id FROM %s common LEFT JOIN (SELECT customer_id_hashed, \
        transaction_dt, trans_order_since_18Q1, units, sales, location_id, department_id FROM %s) pos ON \
        common.customer_id_hashed = pos.customer_id_hashed " % (self.table_filtered_id, self.pos_table)
            clause_where = "WHERE %s;" % append_date_range_condition("pos.transaction_dt",
                                                                     start_date=self.pos_start_date,
                                                                     end_date=self.pos_end_date)
            list_of_clause = [clause_create_table, clause_select, clause_where]
            query = self.concat_clause(list_of_clause=list_of_clause)
            list_of_columns = ["customer_id_hashed", "transaction_dt", "trans_order_since_18Q1", "units", "sales",
                               "location_id", "department_id"]
            self.execute_with_timer(query=query, task="Filter POS", table_name=self.table_filtered_pos,
                                    list_of_columns=list_of_columns)
            return

        def get_max_trans_order_for_each_id():
            self.table_max_trans_order = self.generate_table_name(prefix="max_trans")
            self.execute_dropping_table_with_timer(task="DROP TABLE of Max Transaction Order",
                                                   table_name=self.table_max_trans_order)
            clause_create_table = "CREATE TABLE %s AS " % self.table_max_trans_order
            clause_select = " SELECT customer_id_hashed AS id, MAX(trans_order_since_18Q1) AS max_trans FROM %s \
        GROUP BY customer_id_hashed;" % self.table_filtered_pos
            list_of_clause = [clause_create_table, clause_select]
            query = self.concat_clause(list_of_clause=list_of_clause)
            list_of_columns = ["id", "max_trans"]
            result = self.execute_with_timer(query=query, task="Get Max Transaction Order",
                                             table_name=self.table_max_trans_order, list_of_columns=list_of_columns)
            self.record[self.table_max_trans_order] = result[1]
            return

        def append_date_range_condition(table_and_col_name, start_date="", end_date="", previous_condition=False):
            result = ""
            if not start_date and not end_date:
                return result
            start_date = "'" + start_date + "'" if start_date else start_date
            end_date = "'" + end_date + "'" if end_date else end_date
            if not start_date and end_date:
                result = "%s <= %s" % (table_and_col_name, end_date)
            elif start_date and not end_date:
                result = "%s >= %s" % (table_and_col_name, start_date)
            else:
                result = "(%s BETWEEN %s AND %s)" % (table_and_col_name, start_date, end_date)
            result = "AND " + result if previous_condition else result
            return result

        print("Preprocessing for POS, CRM, and IDs...")
        start = datetime.datetime.now()
        choose_and_filter_id()
        filter_crm()
        filter_pos()
        get_max_trans_order_for_each_id()
        self.validating_record_consistency(
            [self.table_filtered_id, self.table_filtered_crm, self.table_max_trans_order])
        self.execute_dropping_table_with_timer(task="DROP TABLE of CRM ID IF EXISTS",
                                               table_name=self.table_id_from_crm)
        end = datetime.datetime.now()
        print("Preprocessed")
        self.print_time_consuming(task="Preprocessing.", start=start, end=end)

    def retrieve_department_ids(self):
        query = "SELECT DISTINCT(department_id) FROM %s;" % self.pos_table
        self.print_query(query)
        self.department_ids = pd.read_sql(query, con=self.engine).iloc[:, 0].tolist()
        print("Number of Department IDs: %d" % len(self.department_ids))
        return self.department_ids

    def build_table_2_1(self):
        print("Building Table 2-1")
        self.table_2_1 = self.generate_table_name(prefix="all", table_name="pred_pos_2_1") if not \
            self.is_with_dcm else self.generate_table_name(table_name="pred_pos_2_1")
        self.execute_dropping_table_with_timer(task="DROP TABLE of Table 2-1", table_name=self.table_2_1)
        clause_create_table = "CREATE TABLE %s AS " % self.table_2_1
        clause_select = "SELECT t.customer_id_hashed AS id, \
    FLOOR(DATEDIFF(%s, MIN(crm.sign_up_date))/7) AS weeks_since_sign_up, \
    MAX(t.trans_order_since_18Q1) AS total_trans_since_registration, \
    SUM(t.units) AS total_items, \
    SUM(t.sales) AS total_sales, \
    COUNT(DISTINCT (CASE WHEN t.location_id != 6990 THEN t.trans_order_since_18Q1 ELSE 0 END)) AS trans_in_store, \
    COUNT(DISTINCT t.location_id) AS unique_stores, \
    COUNT(DISTINCT (CASE WHEN t.location_id = 6990 THEN t.trans_order_since_18Q1 ELSE 0 END)) AS trans_online,"%("'"+str(self.pos_end_date)+"'")
        clause_pivot = self.generate_pivot_clause(aggregator="COUNT", condition="t.department_id",
                                                  value_true="t.trans_order_since_18Q1", value_false="0",
                                                  is_distinct=True, column_suffix="trans")
        clause_from = "FROM %s t" % self.table_filtered_pos
        clause_join = "LEFT JOIN (SELECT customer_id_hashed, sign_up_date FROM %s) crm" % self.table_filtered_crm
        clause_on = "ON t.customer_id_hashed = crm.customer_id_hashed"
        clause_groupby = "GROUP BY t.customer_id_hashed;"
        list_of_clause = [clause_create_table, clause_select, clause_pivot, clause_from, clause_join, clause_on,
                          clause_groupby]
        query = self.concat_clause(list_of_clause)
        list_of_columns = ["id"]
        result = self.execute_with_timer(query=query, task="Generate TABLE 2-1", table_name=self.table_2_1,
                                         list_of_columns=list_of_columns)
        self.record[self.table_2_1] = result[1]
        self.validating_record_consistency([self.table_filtered_id, self.table_2_1])
        self.columns_table_2_1 = self.get_columns(self.table_2_1)
        return

    def build_table_2_2(self):
        def build_all_views():
            build_view_of_fist_week_transaction()
            build_view_of_recent_one_to_two_transaction()
            return

        def build_view_of_fist_week_transaction():
            clause_create_view = "CREATE OR REPLACE VIEW view_pred_pos_2_2_first_week AS "
            clause_select = "SELECT p.customer_id_hashed AS id, MIN(p.transaction_dt) AS week_1st_trans, MAX(\
        CASE WHEN p.location_id = 6990 THEN 0 ELSE 1 END) AS purchase_channel_1st_trans, SUM(p.sales) AS \
        total_sales_1st_trans, SUM(p.units) AS total_units_1st_trans,"
            clause_pivot = generate_table_2_2_pivot_clause("1st_trans")
            clause_from = "FROM %s p" % self.table_filtered_pos
            clause_where = "WHERE trans_order_since_18Q1 = 1"
            clause_groupby = "GROUP BY customer_id_hashed;"
            list_of_clause = [clause_create_view, clause_select, clause_pivot, clause_from, clause_where,
                              clause_groupby]
            query = self.concat_clause(list_of_clause)
            self.execute_with_timer(query=query, task="Create or Replace View of First Week")
            build_view_columns_map("view_pred_pos_2_2_first_week")
            return

        def build_view_of_recent_one_to_two_transaction():
            all_view_query = [generate_view_of_recent_week_n_transaction(i + 1) for i in range(2)]
            for index, view_query in enumerate(all_view_query):
                self.execute_with_timer(query=view_query, task="Create or Replace View of Recent %d" % (index + 1))
                build_view_columns_map("view_pred_pos_2_2_recent_%s" % NumberToWords(index + 1))
            return

        def generate_view_of_recent_week_n_transaction(week_n):
            suffix = 'recent_%s' % NumberToWords(week_n)
            clause_create_view = "CREATE OR REPLACE VIEW view_pred_pos_2_2_%s AS" % suffix
            clause_select = "SELECT p.customer_id_hashed AS id, MAX(p.transaction_dt) AS week_%s_trans, \
        MAX(CEILING(DATEDIFF(%s, p.transaction_dt)/7)) AS week_counts_to_now_%s, \
        MAX(CASE WHEN p.trans_order_since_18Q1 = 1 THEN 1 ELSE 0 END) AS %s_trans_also_1st, \
        MAX(CASE WHEN p.location_id = 6990 THEN 0 ELSE 1 END) AS purchase_channel_1st_trans_%s, \
        SUM(p.sales) AS total_sales_%s_trans, SUM(p.units) AS total_units_%s_trans," % \
                            (suffix, "'"+str(self.pos_end_date)+"'", suffix, suffix, suffix, suffix, suffix)
            clause_pivot = generate_table_2_2_pivot_clause(suffix)
            clause_from = "FROM %s p" % self.table_filtered_pos
            clause_join = "INNER JOIN %s pos_max" % self.table_max_trans_order
            clause_on = "ON p.customer_id_hashed = pos_max.id"
            clause_where = "WHERE p.trans_order_since_18Q1 = pos_max.max_trans - %s" % (
                    week_n - 1) if week_n != 1 else \
                "WHERE p.trans_order_since_18Q1 = pos_max.max_trans"
            clause_groupby = "GROUP BY p.customer_id_hashed;"
            list_of_clause = [clause_create_view, clause_select, clause_pivot, clause_from, clause_join, clause_on,
                              clause_where, clause_groupby]
            return self.concat_clause(list_of_clause)

        def generate_table_2_2_pivot_clause(column_suffix):
            return self.generate_pivot_clause(aggregator="SUM", condition="p.department_id", value_true="units",
                                              value_false='0', is_distinct=False, column_prefix='total_units',
                                              column_suffix=column_suffix)

        def generate_select_clause():
            first_week = "SELECT f.*, "
            all_recent_n = ", ".join(
                [', '.join(self.map_columns_table_2_2["view_pred_pos_2_2_recent_%s" % NumberToWords(i + 1)][1:]) for
                 i
                 in range(2)])
            return first_week + all_recent_n

        def generate_join_clause():
            first_week = "view_pred_pos_2_2_first_week f"
            all_recent_n_list = [
                "LEFT JOIN view_pred_pos_2_2_recent_%s r%d ON f.id=r%d.id" % (NumberToWords(i + 1), i + 1, i + 1)
                for i
                in range(2)]
            all_recent_n = ' '.join(all_recent_n_list)
            return first_week + ' ' + all_recent_n

        def build_view_columns_map(view_name):
            self.map_columns_table_2_2[view_name] = self.get_columns(view_name)
            return

        print("Building Table 2-2")
        self.table_2_2 = self.generate_table_name(prefix="all", table_name="pred_pos_2_2") if not \
            self.is_with_dcm else self.generate_table_name(table_name="pred_pos_2_2")
        self.execute_dropping_table_with_timer(task="DROP TABLE of Table 2-2", table_name=self.table_2_2)
        build_all_views()
        clause_create_table = "CREATE TABLE %s AS " % self.table_2_2
        clause_select = generate_select_clause()
        clause_from = "FROM "
        clause_join = generate_join_clause()
        clause_end = ";"
        list_of_clause = [clause_create_table, clause_select, clause_from, clause_join, clause_end]
        query = self.concat_clause(list_of_clause)
        list_of_columns = ["id"]
        result = self.execute_with_timer(query=query, task="Generate TABLE 2-2", table_name=self.table_2_2,
                                         list_of_columns=list_of_columns)
        self.record[self.table_2_2] = result[1]
        self.validating_record_consistency([self.table_filtered_id, self.table_2_2])
        self.columns_table_2_2 = self.get_columns(self.table_2_2)
        return
    def build_table_1_with_split(self): # jian 0807, "processing_part_2.ipynb"
        with open('./config.json', 'rb') as f:
            dict_config = json.load(f)
        high_date=datetime.datetime.strptime(dict_config['crm_end_date'],"%Y-%m-%d").date()
        if dict_config['recent_n_month']:
            recent_n_month=dict_config['recent_n_month']
            pos_start_date_id_filter = str(high_date-datetime.timedelta(days=int(np.ceil(365*recent_n_month/12))))
        else:
            pos_start_date_id_filter = dict_config["pos_start_date"]
                    
        sql_str_high_date="'%s'"%str(high_date)
        sql_str_lastweekstart_date="'%s'"%str(high_date-datetime.timedelta(days=6))
        # sql_sign_up_start_date="'%s'"%str(sign_up_start_date)
        sql_POS_start_date="'%s'"%str(pos_start_date_id_filter)
        str_week_end_d=str(high_date).replace("-","")


        path_store_list=glob.glob(self.folder_store_list+"*.txt")
        path_store_list_ahead=[x for x in path_store_list if "MediaStormStores%s"%str_week_end_d[:6] in x][0]
        path_store_list_after=[x for x in path_store_list if "MediaStormStores%s"%str(int(str_week_end_d[:6])+1) in x][0]
        # 
        TA_zips=pd.ExcelFile(self.path_TA_excel)
        TA_zips=TA_zips.parse("view_by_store",dtype=str)

        df_temporary=TA_zips[['location_id','trans_P_zips_70_within_TA','trans_S_zips_70_within_TA','zips_in_10']]
        df_zip_by_store=pd.DataFrame()

        for ind,row in df_temporary.iterrows():
            location_id=str(row['location_id'])
            P_zips=eval(row['trans_P_zips_70_within_TA'])
            S_zips=eval(row['trans_S_zips_70_within_TA'])
            zip_10=eval(row['zips_in_10'])
            

            df_P=pd.DataFrame(zip([location_id]*len(P_zips),P_zips))
            if len(df_P)>0:
                df_P.columns=['location_id','zip_cd']
                df_P['zip_type']="P"
                
            df_S=pd.DataFrame(zip([location_id]*len(S_zips),S_zips))
            if len(df_S)>0:
                df_S.columns=['location_id','zip_cd']
                df_S['zip_type']="S"
            
            df_10=pd.DataFrame(zip([location_id]*len(zip_10),zip_10))
            if len(df_10)>0:
                df_10.columns=['location_id','zip_cd']
                df_10['zip_type']="zip_10"
            
            df_zip_by_store=df_zip_by_store.append(df_P).append(df_S).append(df_10)
        df_zip_by_store['location_id']=df_zip_by_store['location_id'].astype(str)
        df_store_list=df_store_list[['location_id','latitude_meas','longitude_meas']]
        df_store_zip=pd.merge(df_store_list,df_zip_by_store,on="location_id",how="left")
        df_store_zip_new=df_store_zip[pd.isnull(df_store_zip['zip_cd'])]
        df_store_zip_existing=df_store_zip[pd.notnull(df_store_zip['zip_cd'])]

        df_store_zip_new_no_loc=df_store_zip_new[df_store_zip_new['latitude_meas']==0]
        df_store_zip_new_with_loc=df_store_zip_new[df_store_zip_new['latitude_meas']!=0]
        df_store_zip_new_with_loc=df_store_zip_new_with_loc[['location_id','latitude_meas','longitude_meas']]
        df_store_zip_new_no_loc=df_store_zip_new_no_loc[['location_id','latitude_meas','longitude_meas']]
        if len(df_store_zip_new_no_loc)>0:
            store_list_later=[x for x in list_store_list if x.split("MediaStormStores")[1][:6]>str_week_end_d]
            store_list_later=sorted(store_list_later,key=lambda x: os.stat(x).st_mtime)
            for file in store_list_later:
                df=pd.read_csv(file,dtype=str,sep="|",usecols=['location_id','latitude_meas','longitude_meas'])
                df=df[['location_id','latitude_meas','longitude_meas']]
                df['latitude_meas']=df['latitude_meas'].astype(float)
                df['longitude_meas']=df['longitude_meas'].astype(float)
                df['location_id']=df['location_id'].astype(str)
                df=df[df['location_id'].isin(df_store_zip_new_no_loc['location_id'].tolist())]
                df=df[df['latitude_meas']!=0]
                df_store_zip_new_with_loc=df_store_zip_new_with_loc.append(df)
                df_store_zip_new_no_loc=df_store_zip_new_no_loc[~df_store_zip_new_no_loc['location_id'].isin(df['location_id'].tolist())]
                if len(df_store_zip_new_no_loc)==0:
                    break
            df_store_zip_new=df_store_zip_new_with_loc.reset_index()
            del df_store_zip_new['index']
            if df_store_zip_new_with_loc:
                del df_store_zip_new_with_loc
            if df_store_zip_new_no_loc:
                del df_store_zip_new_no_loc

        zip_centers=json.load(open(self.path_json_zip_center,"r"))
        if len(df_store_zip_new)>0:
            del df_store_zip_new['zip_cd']
            del df_store_zip_new['zip_type']
            
            df_all_new_zip=pd.DataFrame()
            for i,row in df_store_zip_new.iterrows():
                store_coor=(row['latitude_meas'],row['longitude_meas'])
                store_num=row['location_id']
                list_store_zip=[]
                for zip_cd, v in zip_centers.items():
                    dist=haversine(store_coor,v,unit="mi")
                    if dist<=10:
                        list_store_zip.append(zip_cd)
                df=pd.DataFrame({"zip_cd":list_store_zip,"zip_type":["zip_10"]*len(list_store_zip)},index=[store_num]*len(list_store_zip))
                df=df.reset_index().rename(columns={"index":"location_id"})
                df_all_new_zip=df_all_new_zip.append(df)

            df_store_zip_new=pd.merge(df_store_zip_new,df_all_new_zip,on="location_id",how="left")
            
            df_store_zip=df_store_zip_existing.append(df_store_zip_new)
        else:
            df_store_zip=df_store_zip_existing
        df_zip_type=df_store_zip[['zip_cd','zip_type']].drop_duplicates()
        df_zip_type=df_zip_type.sort_values(['zip_cd','zip_type'])
        print(df_zip_type['zip_type'].unique().tolist())
        df_unique_zip_type=df_zip_type.drop_duplicates("zip_cd")

        list_P_zips=df_zip_type[df_zip_type['zip_type']=="P"]['zip_cd'].tolist()
        list_S_zips=df_zip_type[df_zip_type['zip_type']=="S"]['zip_cd'].tolist()
        list_10_zips=df_zip_type[df_zip_type['zip_type']=="zip_10"]['zip_cd'].tolist()

        df_store_list=df_store_zip[['location_id','latitude_meas','longitude_meas']].drop_duplicates().reset_index()
        del df_store_list['index']
        df_store_list=df_store_zip[['location_id','latitude_meas','longitude_meas']].drop_duplicates().reset_index()
        del df_store_list['index']
        # 
        from multiprocessing import Pool
        processors=20

        list_all_zips=list(zip_centers.keys())
        len_chunck=int(np.ceil(len(list_all_zips)/processors))
        list_of_input_all_us_zip_list=[]

        for i in range(processors):
            l=list_all_zips[i*len_chunck:(i+1)*len_chunck]
            list_of_input_all_us_zip_list.append(l)

        def get_dist_output_df(input_zip_list):
            df_output=pd.DataFrame()
            
            for zip_cd in input_zip_list:
                z_centroid=zip_centers[zip_cd]
                min_dist=np.inf
                nearest_store=None
                
                for i,row in df_store_list.iterrows():
                    store=row['location_id']
                    store_loc=(row['latitude_meas'], row['longitude_meas'])
                    dist=haversine(z_centroid,store_loc,unit="mi")
                    if dist<=min_dist:
                        min_dist=dist
                        nearest_store=store
                df=pd.DataFrame({"nearest_BL_store":nearest_store,"nearest_BL_dist":min_dist},index=[zip_cd])
                df=df.reset_index().rename(columns={"index":"zip_cd"})
                df_output=df_output.append(df)
            return df_output


        p = Pool(processors)
        result=p.map(get_dist_output_df, list_of_input_all_us_zip_list)
        df_zips_with_BL_store=pd.DataFrame()
        for res in result:
            if res is not None:
                df_zips_with_BL_store=df_zips_with_BL_store.append(res)
        p.close()
        p.join()
        print(df_zips_with_BL_store.shape,df_zips_with_BL_store['zip_cd'].nunique(),df_zips_with_BL_store['nearest_BL_store'].nunique())
        df_zips_with_BL_store['zip_cd']=df_zips_with_BL_store['zip_cd'].astype(str)
        df_zips_with_BL_store['zip_cd']=df_zips_with_BL_store['zip_cd'].apply(lambda x: x.zfill(5))

        # IVs
        print(datetime.datetime.now())
        df_1=pd.read_sql("select t1.customer_id_hashed, sign_up_channel, sign_up_location, customer_zip_code from BL_Rewards_Master as t1 \
        right join %s as t2 on t1.customer_id_hashed=t2.customer_id_hashed;"%self.table_filtered_crm, con=self.engine)


        df_1_len=df_1.shape[0]
        df_1_id_nunique=df_1['customer_id_hashed'].nunique()
        print("df_1_len",df_1_len)
        print("df_1_id_nunique",df_1_id_nunique)
        print(datetime.datetime.now())

        df_1['customer_zip_code']=df_1['customer_zip_code'].astype(str)
        df_1['customer_zip_code']=df_1['customer_zip_code'].apply(lambda x: x.split("-")[0].split(" ")[0].zfill(5)[:5])
        # df_1['sign_up_date']=pd.to_datetime(df_1['sign_up_date'],format="%Y-%m-%d").dt.date
        # df_1['weeks_since_sign_up']=df_1['sign_up_date'].apply(lambda x: int(np.ceil((high_date-x).days/7)))
        df_1['P_zip']=np.where(df_1['customer_zip_code'].isin(list_P_zips),1,0)
        df_1['S_zip']=np.where(df_1['customer_zip_code'].isin(list_S_zips),1,0)
        df_1['else_10_zip']=np.where(df_1['customer_zip_code'].isin(list_10_zips),1,0)
        # del df_1['customer_zip_code']
        df_1['signed_online']=np.where(df_1['sign_up_channel']=="STORE",0,1)
        del df_1['sign_up_channel']

        df_1['sign_up_location']=df_1['sign_up_location'].fillna("-1")
        df_1['sign_up_location']=df_1['sign_up_location'].astype(float)
        df_1['sign_up_location']=df_1['sign_up_location'].astype(int).astype(str)

        df_copy_sign_up=df_1[['sign_up_location','customer_zip_code']].drop_duplicates()
        df_copy_sign_up=df_copy_sign_up.reset_index()
        del df_copy_sign_up['index']

        # distance to sign up stores
        df_store_all=pd.DataFrame(columns=['location_id','latitude_meas','longitude_meas'])

        list_all_stores=glob.glob(self.folder_store_list+"*.txt")
        list_all_stores=[x for x in list_all_stores if "MediaStormStores" in x]
        list_all_stores=sorted(list_all_stores,key=lambda x :x.split("MediaStormStores")[1][:8])
        list_all_stores=[x for x in list_all_stores if x.split("MediaStormStores")[1][:8]<=str(high_date+datetime.timedelta(days=2)).replace("-","")]
        list_all_stores.reverse()

        for file in list_all_stores:
            df=pd.read_table(file,dtype=str,sep="|",usecols=['location_id','latitude_meas','longitude_meas'])
            df=df[['location_id','latitude_meas','longitude_meas']]
            df['latitude_meas']=df['latitude_meas'].astype(float)                   
            df['longitude_meas']=df['longitude_meas'].astype(float)   
            df=df[~df['location_id'].isin(['145','6990'])]
            df=df[~df['location_id'].isin(df_store_all['location_id'].tolist())]
            df_store_all=df_store_all.append(df)
        df_store_all['store_coor']=df_store_all[['latitude_meas','longitude_meas']].values.tolist()                      
        dict_store_all=df_store_all.set_index("location_id").to_dict()['store_coor']
        df_copy_sign_up['distc_to_sign_up']=np.nan
        for i,row in df_copy_sign_up.iterrows():
            try:
                store_coor=dict_store_all[row['sign_up_location']]
                zip_center=zip_centers[row['customer_zip_code']]
                dist=haversine(store_coor,zip_center,unit="mi")
                df_copy_sign_up.loc[i,"distc_to_sign_up"]=dist
                
            except:
                continue
        df_1=pd.merge(df_1,df_copy_sign_up,on=['sign_up_location','customer_zip_code'],how="left")
        #
        list_unsub=glob.glob(self.folder_email_unsub+"*.csv")
        df_unsub_files=pd.DataFrame({"file_path":list_unsub})
        df_unsub_files['date']=df_unsub_files['file_path'].apply(lambda x: x.split("ile_Refresh__")[1][:8])
        df_unsub_files['date']=pd.to_datetime(df_unsub_files['date']).dt.date
        df_unsub_files['day_diff']=abs(df_unsub_files['date']-high_date)
        path_unsub=df_unsub_files[df_unsub_files['day_diff']==df_unsub_files['day_diff'].min()]['file_path'].values.tolist()[0]
        ###### 
        list_unsunsribe_ids=pd.read_csv(path_unsub,
                                 dtype=str,usecols=['customersummary_c_primaryscnhash'])['customersummary_c_primaryscnhash'].unique().tolist()

        print(len(list_unsunsribe_ids))
        df_1['email_unsub_label']=np.where(df_1['customer_id_hashed'].isin(list_unsunsribe_ids),1,0)
        del list_unsunsribe_ids
        df_zips_with_BL_store=df_zips_with_BL_store.rename(columns={"zip_cd":"customer_zip_code"})
        df_1=pd.merge(df_1,df_zips_with_BL_store,on="customer_zip_code",how="left")
        df_1=df_1.reset_index()
        del df_1['index']
        df_1=df_1.reset_index()
        del df_1['index']
        df_1=df_1.reset_index()
        dv_start_date=high_date+datetime.timedelta(days=1)
        dv_end_date=high_date+datetime.timedelta(days=28)

        str_sql_dv_start_date="'"+str(dv_start_date)+"'"
        str_sql_dv_end_date="'"+str(dv_end_date)+"'"
        print(str_sql_dv_start_date,str_sql_dv_end_date)
        print(datetime.datetime.now())
        df_dvs=pd.read_sql("select customer_id_hashed, transaction_dt from Pred_POS_Department \
        where transaction_dt between %s and %s and sales >0"%(str_sql_dv_start_date,str_sql_dv_end_date),con=self.engine).drop_duplicates()
        print(datetime.datetime.now())

        def week_end_dt(date_input):
            weekday_int=date_input.weekday()
            if weekday_int==6:
                return date_input+datetime.timedelta(days=6)
            else:
                return date_input+datetime.timedelta(days=5-weekday_int)
        df_dvs['week_end_dt']=df_dvs['transaction_dt'].apply(week_end_dt)
        df_dvs=df_dvs[['customer_id_hashed','week_end_dt']].drop_duplicates()
        list_unique_weeks=df_dvs['week_end_dt'].unique().tolist()
        list_unique_weeks.sort()
        df_dv_binary=df_dvs[df_dvs['week_end_dt']==list_unique_weeks[0]][['customer_id_hashed']]
        df_dv_binary['DV_cumulative_week_updated_1']=1
        for i in range(1,4):
            w=list_unique_weeks[i]
            df=df_dvs[df_dvs['week_end_dt']<=w][['customer_id_hashed']].drop_duplicates()
            df['DV_cumulative_week_updated_%d'%(i+1)]=1
            df_dv_binary=pd.merge(df_dv_binary,df,on="customer_id_hashed",how="outer")
            print(w,datetime.datetime.now())
        df_dv_binary=df_dv_binary.fillna(0)

        df_1=pd.merge(df_dv_binary,df_1,on="customer_id_hashed",how="right")

        for i in range(4):
            df_1['DV_cumulative_week_updated_%d'%(i+1)]=df_1['DV_cumulative_week_updated_%d'%(i+1)].fillna(0)
            
        print(df_1.shape,df_1['customer_id_hashed'].nunique())
        if "index" in df_1.columns.tolist():
            del df_1['index']

        df_1.head(4)

        self.table_crm_id_list_train="crm_table_id_list_train_%s"%str_week_end_d
        self.table_crm_id_list_test="crm_table_id_list_test_%s"%str_week_end_d
        self.table_df_1="table_pred_1_crm_from_up_to_%s"%str_week_end_d

        import random

        len_df_1=len(df_1)
        train_sample_size=10**6
        test_ratio=0.25
        if len_df_1>train_sample_size/(1-test_ratio):
            list_ind_train=random.sample(range(len_df_1), train_sample_size)
        else:
            list_ind_train=random.sample(range(len_df_1), int(len_df_1*(1-test_ratio)))
        df_1_train=df_1[['customer_id_hashed']][df_1['index'].isin(list_ind_train)]
        df_1_test=df_1[['customer_id_hashed']][~df_1['index'].isin(list_ind_train)]

        print("df_1_train.shape",df_1_train.shape)
        print("df_1_test.shape",df_1_test.shape)

        dtype_id={"customer_id_hashed": sql.types.VARCHAR(length=64)}
        df_1_train.to_sql(name=self.table_crm_id_list_train,
            con=self.engine, index=False, if_exists="replace", dtype=dtype_id)
        df_1_test.to_sql(name=self.table_crm_id_list_test,
            con=self.engine, index=False, if_exists="replace", dtype=dtype_id)

        dtype_df_1={
        'customer_id_hashed':sql.types.VARCHAR(length=64),
        'DV_cumulative_week_updated_1':sql.types.Integer,
        'DV_cumulative_week_updated_2':sql.types.Integer,
        'DV_cumulative_week_updated_3':sql.types.Integer,
        'DV_cumulative_week_updated_4':sql.types.Integer,
        'sign_up_location':sql.types.VARCHAR(length=4),
        'customer_zip_code':sql.types.VARCHAR(length=5),
        'P_zip':sql.types.Integer,
        'S_zip':sql.types.Integer,
        'else_10_zip':sql.types.Integer,
        'signed_online':sql.types.Integer,
        'distc_to_sign_up':sql.types.Float,
        'email_unsub_label':sql.types.Integer,
        'nearest_BL_store':sql.types.VARCHAR(length=4),
        'nearest_BL_dist':sql.types.Float
        }
        df_1.to_sql(name=self.table_df_1,
            con=self.engine, index=False, if_exists="replace", dtype=dtype_df_1)

        self.create_index(table_name=self.table_crm_id_list_train, list_of_columns=["customer_id_hashed"])
        self.create_index(table_name=self.table_crm_id_list_test, list_of_columns=["customer_id_hashed"])
        self.create_index(table_name=self.table_df_1, list_of_columns=["customer_id_hashed"])


    def output_to_csv(self):
        def output_table_2_1():
            self.csv_table_2_1 = generate_valid_path(path=self.base_directory, file_name=self.table_2_1)
            concat_columns = ', '.join(['"' + columns + '"' for columns in self.columns_table_2_1])
            clause_select_header = "SELECT %s" % concat_columns
            clause_union = "UNION"
            clause_select_rows = "SELECT * FROM %s ORDER BY RAND()" % self.table_2_1
            clause_outfile = "INTO OUTFILE '%s'" % self.csv_table_2_1
            clause_ending = '''FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY %s;''' % repr('\n')
            list_of_clause = [clause_select_header, clause_union, clause_select_rows, clause_outfile, clause_ending]
            query = self.concat_clause(list_of_clause=list_of_clause)
            self.execute_with_timer(query=query, task="Output Table 2-1")
            print_path(table_name=self.table_2_1, filepath=self.csv_table_2_1)
            return

        def output_table_2_2():
            self.csv_table_2_2 = generate_valid_path(path=self.base_directory, file_name=self.table_2_2)
            clause_select_rows = "SELECT * FROM %s ORDER BY RAND()" % self.table_2_2
            clause_outfile = "INTO OUTFILE '%s'" % self.csv_table_2_2
            clause_ending = '''FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY %s;''' % repr('\n')
            list_of_clause = [clause_select_rows, clause_outfile, clause_ending]
            query = self.concat_clause(list_of_clause=list_of_clause)
            self.execute_with_timer(query=query, task="Output Table 2-2")
            output_path = add_header_for_file(filepath=self.csv_table_2_2, header=self.columns_table_2_2,
                                              table_name=self.table_2_2)
            print_path(table_name=self.table_2_2, filepath=output_path, with_header=True)
            return

        def add_header_for_file(filepath, header, table_name):
            concat_header = ', '.join(header)
            output_path = generate_path_of_file_with_header(filepath=filepath)
            command = '''{ echo "%s"; cat %s; } > %s''' % (concat_header, filepath, output_path)
            print("Command Line for Adding Header of %s" % filepath)
            print(command)
            os.system(command)
            print("CSV File of %s with Headers: %s" % (output_path, table_name))
            return output_path

        def generate_path_of_file_with_header(filepath):
            base_dir, filename = '/'.join(filepath.split('/')[:-1]), filepath.split('/')[-1]
            if "new_" != filename[:4]:
                return base_dir + '/' + 'new_' + filename
            elif not filename.split('_')[1].isdigit():
                filename = "new_2_" + filename.split('_')[1]
            else:
                version_increment = int(filename.split('_')[1]) + 1
                filename = 'new_' + str(version_increment) + '_' + filename.split('_')[2]
            return filename

        def generate_valid_path(path, file_name):
            output_path = path + file_name + '.csv'
            while os.path.exists(output_path):
                if "new_" != file_name[:4]:
                    return path + file_name + '.csv'
                elif not file_name.split('_')[1].isdigit():
                    file_name = "new_2_" + file_name.split('_')[1]
                else:
                    version_increment = int(file_name.split('_')[1]) + 1
                    file_name = "new_" + str(version_increment) + '_' + file_name.split('_')[2]
                output_path = path + file_name + '.csv'
            return output_path

        def print_path(table_name, filepath, with_header=False):
            if with_header:
                print("Path of Table with its Header- %s: %s" % (table_name, filepath))
            else:
                print("Path of Table - %s: %s" % (table_name, filepath))

        output_table_2_1()
        output_table_2_2()

    def execute_with_timer(self, query, task, table_name="", list_of_columns=None):
        if not list_of_columns:
            list_of_columns = []
        self.print_query(query)
        start = datetime.datetime.now()
        with self.engine.connect() as connection:
            result = connection.execute(query)
        end = datetime.datetime.now()
        self.print_time_consuming(task, start, end)
        rowcount = result.rowcount
        if table_name:
            self.print_number_of_records(rowcount)
            self.create_index(table_name, list_of_columns)
        print('----')
        result.close()
        return table_name, rowcount

    def create_index(self, table_name, list_of_columns):
        columns = ', '.join(list_of_columns)
        query = "CREATE INDEX id_index ON %s(%s)" % (table_name, columns)
        self.print_query(query)
        start = datetime.datetime.now()
        with self.engine.connect() as connection:
            result = connection.execute(query)
        end = datetime.datetime.now()
        self.print_time_consuming("Create Index", start, end)
        result.close()
        return

    def execute_dropping_table_with_timer(self, table_name, task):
        if table_name:
            query = "DROP TABLE IF EXISTS %s" % table_name
            self.print_query(query)
            start = datetime.datetime.now()
            with self.engine.connect() as connection:
                result = connection.execute(query)
            end = datetime.datetime.now()
            self.print_time_consuming(task, start, end)
            result.close()
            return
        else:
            print("No table was dropped since not existing")

    def generate_table_name(self, prefix="", table_name="", source_table_name="", is_from_source=False):
        result = ""
        if not self.is_with_dcm:
           if self.sample_ratio<1:
              percentage = NumberToWords((self.sample_ratio * 100)) + '_' + "percentage"
              result = percentage + "_id_from_" + source_table_name if is_from_source else percentage + '_id'
           else:
              percentage = "all"
              result = percentage + "_id_from_" + source_table_name if is_from_source else percentage + '_id'
        result = result + "_" + table_name if table_name else result
        result = "WE" + result if self.is_with_dcm else "NE" + result
        result += self.append_date_range_suffix(crm_or_pos="pos", start_date=self.pos_start_date,
            end_date=self.pos_end_date)
        return prefix + '_' + result if prefix else result

    def append_date_range_suffix(self, crm_or_pos, start_date, end_date):
        if not start_date and not end_date:
            return ""
        if not start_date and end_date:
            result = "_" + crm_or_pos + "_until_%s" % end_date
        elif start_date and not end_date:
            result = "_" + crm_or_pos + "_from_%s" % start_date
        else:
            result = "_" + crm_or_pos + "_from_%s_to_%s" % (start_date, end_date)
        result = result.replace('-', "")
        return result

    def generate_pivot_clause(self, aggregator, condition, value_true, value_false, is_distinct=False,
                              column_prefix="",
                              column_suffix=""):
        pivot_list = []
        size = len(self.department_prefix)
        for i in range(size):
            column_name = self.add_prefix_and_suffix_of_columns(columns=self.department_prefix[i],
                                                                prefix=column_prefix,
                                                                suffix=column_suffix)
            if is_distinct:
                cur_pivot = "%s(DISTINCT (CASE WHEN %s = %s THEN %s ELSE %s END)) AS %s" % (
                    aggregator, condition, str(self.department_ids[i]), value_true, value_false, column_name)
            else:
                cur_pivot = "%s(CASE WHEN %s = %s THEN %s ELSE %s END) AS %s" % (
                    aggregator, condition, str(self.department_ids[i]), value_true, value_false, column_name)
            if i == 0:
                cur_pivot = "        " + cur_pivot
            pivot_list.append(cur_pivot)
        return ", ".join(pivot_list)

    def add_prefix_of_department(self):
        self.department_prefix = ["department" + '_' + dept_id if dept_id != "-1" else "department_minus_one" for
                                  dept_id in self.department_ids]
        return self.department_prefix

    def add_prefix_and_suffix_of_columns(self, columns, prefix="", suffix=""):
        result = columns
        result = prefix + '_' + columns if prefix else result
        result = columns + '_' + suffix if suffix else result
        return result

    def validating_record_consistency(self, list_of_record):
        cur_dict = {}
        for record_name in list_of_record:
            cur_dict.update({record_name: self.record[record_name]})
        set_record = set(cur_dict.values())
        if len(set_record) != 1:
            message = "Table and Row Counts: %s" % cur_dict
            raise ValueError("Records Are Not Consistent! %s" % message)

    def get_columns(self, table_or_view_name):
        query = "DESC %s;" % table_or_view_name
        df = pd.read_sql(query, con=self.engine)
        return df["Field"].tolist()

    def read_sql_to_pandas_with_timer(self, query):
        self.print_query(query=query)
        start = datetime.datetime.now()
        df = pd.read_sql(sql=query, con=self.engine)
        end = datetime.datetime.now()
        self.print_number_of_records(number=len(df))
        self.print_time_consuming(task="MySQL Query to Pandas DataFrame", start=start, end=end)
        return df

    def write_pandas_to_sql_with_timer(self, dataframe, table_name, dtype):
        start = datetime.datetime.now()
        dataframe.to_sql(name=table_name, con=self.engine, index=False, if_exists="replace", dtype=dtype)
        end = datetime.datetime.now()
        self.print_time_consuming(task="Save %s to SQL" % table_name, start=start, end=end)
        return table_name

    def concat_clause(self, list_of_clause=None):
        if not list_of_clause:
            list_of_clause = []
        return ' '.join(list_of_clause)

    def concat_columns(self, list_of_columns=None):
        if not list_of_columns:
            list_of_columns = []
        return ', '.join(list_of_columns)

    def print_number_of_records(self, number):
        print("Number of Records: %d" % number)
        return

    def print_time_consuming(self, task, start, end):
        print("Time Consuming - %s: %s" % (task, (end - start)))
        return

    def print_query(self, query):
        print("Query: %s" % query)
        return
