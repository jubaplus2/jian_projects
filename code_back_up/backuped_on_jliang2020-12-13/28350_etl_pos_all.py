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
        # MySQL validator
        self.record = {}
        # Output
        self.columns_table_2_1 = list()
        self.columns_table_2_2 = list()
        self.map_columns_table_2_2 = dict()
        self.base_directory = ""
        self.csv_table_2_1 = ""
        self.csv_table_2_2 = ""

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
        self.output_to_csv()
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
        self.crm_start_date = data["crm_start_date"]
        self.crm_end_date = data["crm_end_date"]
        self.is_with_dcm = data["is_with_dcm"]
        self.sample_ratio = data["sample_ratio"]
        self.database_update_period = data["database_update_period"]
        self.base_directory = data["base_directory"]
        if self.base_directory[-1] != '/':
            self.base_directory += '/'
        end = datetime.datetime.now()
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

            if self.sample_ratio<1:
             clause_where = "WHERE RAND() < %f %s;" % (
                 self.sample_ratio, append_date_range_condition(
                     "crm.sign_up_date", start_date=self.crm_start_date, end_date=self.crm_end_date,
                     previous_condition=True))
            else:
                clause_where = "WHERE %s;" %append_date_range_condition(
                "crm.sign_up_date", start_date=self.crm_start_date, end_date=self.crm_end_date,
                previous_condition=True)[4:]

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
                                                                     start_date=self.pos_start_date,
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
    FLOOR(DATEDIFF(%s, MIN(crm.sign_up_date))/7) AS week_diff, \
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
            clause_select = "SELECT p.customer_id_hashed AS id, MIN(p.transaction_dt) AS week_1st_trans, ANY_VALUE(\
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
            clause_select = "SELECT p.customer_id_hashed AS id, ANY_VALUE(p.transaction_dt) AS week_%s_trans, \
        ANY_VALUE(CEILING(DATEDIFF(%s, p.transaction_dt)/7)) AS week_counts_to_now_%s, \
        ANY_VALUE(CASE WHEN p.trans_order_since_18Q1 = 1 THEN 1 ELSE 0 END) AS %s_trans_also_1st, \
        ANY_VALUE(CASE WHEN p.location_id = 6990 THEN 0 ELSE 1 END) AS purchase_channel_1st_trans_%s, \
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

    def output_to_csv(self):
        def output_table_2_1():
            self.csv_table_2_1 = generate_valid_path(path=self.base_directory, file_name=self.table_2_1)
            concat_columns = ', '.join(['"' + columns + '"' for columns in self.columns_table_2_1])
            clause_select_header = "SELECT %s" % concat_columns
            clause_union = "UNION"
            clause_select_rows = "SELECT * FROM %s" % self.table_2_1
            clause_outfile = "INTO OUTFILE '%s'" % self.csv_table_2_1
            clause_ending = '''FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY %s;''' % repr('\n')
            list_of_clause = [clause_select_header, clause_union, clause_select_rows, clause_outfile, clause_ending]
            query = self.concat_clause(list_of_clause=list_of_clause)
            self.execute_with_timer(query=query, task="Output Table 2-1")
            print_path(table_name=self.table_2_1, filepath=self.csv_table_2_1)
            return

        def output_table_2_2():
            self.csv_table_2_2 = generate_valid_path(path=self.base_directory, file_name=self.table_2_2)
            clause_select_rows = "SELECT * FROM %s" % self.table_2_2
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
