import pandas as pd
import streamlit as st
from streamlit.logger import get_logger
from dotenv import load_dotenv
import os
import logging
from datetime import datetime, timedelta
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

st.set_page_config(page_title="Write Encrypted Table Demo")

def format_values():
	t_reformatted = "{:,}".the_value
	

load_dotenv()
auth_info = {
	"account": os.environ["account_name"],
	"user": os.environ["account_user"],
	"password": os.environ["account_password"],
	"role": os.environ["account_role"],
	"schema": os.environ["account_schema"],
	"database": os.environ["account_database"],
	"warehouse": os.environ["account_warehouse"]
}

logger = get_logger(__name__)
the_time = datetime.now()


m_session1 = Session.builder.configs(auth_info).create()

m_session2 = Session.builder.configs(auth_info).create()
record_count = 0

wh_size = st.radio("Warehouse Size?", ["NONE","XSMALL", "SMALL", "MEDIUM", "LARGE", "XLARGE", "XXLARGE"], disabled = False, horizontal = True)

if wh_size != "NONE":
	# m_session1.sql("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 2200").collect()
	wh_info = m_session1.get_current_warehouse()
	m_session1.sql("ALTER WAREHOUSE {} SET WAREHOUSE_SIZE = {} WAIT_FOR_COMPLETION = TRUE".format(wh_info, wh_size)).collect()

	plain_text_table = os.environ["100m_plain_text_table"]
	# Display sample set of the matched table in the clear
	t_matches_df = m_session1.table(plain_text_table).limit(15)

	st.dataframe(t_matches_df.limit(20).to_pandas())

	m_session1.sql("{}".format(os.environ["userkeys"])).collect()

	# Create a dataframe representing the entire matched data set
	matches_df = m_session1.table(plain_text_table)
	# Create a new dataframe representing the encrypted version of the matched data set

	enc_matches_df = m_session1.sql("SELECT 'KEY678901' as keyname,\
								 	{} as {} \
									FROM {}".format("user_id", "user_id", plain_text_table))
	# Count the records in the encrypted matches table.  Diaplay the time it took and the record count
	t_start = datetime.now()
	n_records = enc_matches_df.count()

	# st.write("Overlap matches: {:,}".format(n_records))

	# Display sample set of the encrypted matched table
	st.dataframe(enc_matches_df.limit(20).to_pandas())
	e_table_name = plain_text_table + "_{}_MATCHED_ID".format(t_start.strftime("%H_%M_%S"))
	enc_matches_df.write.mode("overwrite").save_as_table(table_name=e_table_name, table_type='transient')
	t_end = datetime.now()
	the_delta =  t_end.strptime(t_end.strftime("%H:%M:%S"), "%H:%M:%S") - t_start.strptime(t_start.strftime("%H:%M:%S"), "%H:%M:%S")
	t_timing_statement = "Start Time: {} / End Time: {}\n | Total Query Time: {}\n".format(t_start.strftime("%H:%M:%S"), t_end.strftime("%H:%M:%S"), the_delta)
	c1, c2, c3 = st.columns(3)
	c1.metric("Start Time", "{}".format(t_start.strftime("%H:%M:%S")))
	c2.metric("End Time", "{}".format(t_end.strftime("%H:%M:%S")))
	try:
		if not st.session_state["ne_previous_delta"]:
			c3.metric("Run Time", "{}".format(the_delta))
		else:
			c3.metric("Run Time", "{}".format(the_delta), "{:.0%} | vs Previous Run".format(100+(float(the_delta.seconds)/(float(st.session_state["ne_previous_delta"])))))
			# st.write("Else clause hit")
	except:
		c3.metric("Run Time", "{}".format(the_delta))
		st.session_state["ne_previous_delta"] = the_delta.seconds
		# st.write("Exception hit")
	# st.write(t_timing_statement)
	c4, c5 = st.columns(2)
	c4.metric("Record Count", "{:,}".format(enc_matches_df.count()))
	c5.metric("Warehouse Size", "{}".format(wh_size))
