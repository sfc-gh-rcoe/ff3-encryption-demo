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

# Display sample set of the matched table in the clear
t_matches_df = m_session1.table(st.session_state["matched_table_name"]).limit(15)

st.dataframe(t_matches_df.to_pandas())

m_session1.sql("{}".format(os.environ["userkeys"])).collect()

# Create a dataframe representing the entire matched data set
matches_df = m_session1.table(st.session_state["matched_table_name"])
# Create a new dataframe representing the encrypted version of the matched data set
# enc_matches_df = m_session1.sql("SELECT 'KEY678901', encrypt_ff3_string_pass3('KEY678901', {}, {}),\
# 								encrypt_ff3_string_pass3('KEY678901', {}, {}),\
# 								encrypt_ff3_string_pass3('KEY678901', {}, {}),\
# 								encrypt_ff3_string_pass3('KEY678901', {}, {}),\
# 								encrypt_ff3_string_pass3('KEY678901', {}, {}),\
# 								encrypt_ff3_string_pass3('KEY678901', {}, {}) FROM {}"\
# 								.format("name", os.environ["userkeys"],\
# 				"phone", os.environ["userkeys"],\
# 				"EMAILADDRESS", os.environ["userkeys"],\
# 				"fname", os.environ["userkeys"],\
# 				"lname", os.environ["userkeys"],\
# 				"displayname", os.environ["userkeys"], st.session_state["matched_table_name"]))

enc_matches_df = m_session1.sql("SELECT 'KEY678901' as keyname,\
								ff3_testing_db.ff3_testing_schema.encrypt_ff3_string_pass3('KEY678901', {}, $userkeys) as {}, \
								ff3_testing_db.ff3_testing_schema.encrypt_ff3_string_pass3('KEY678901', {}, $userkeys) as {}, \
								ff3_testing_db.ff3_testing_schema.encrypt_ff3_string_pass3('KEY678901', {}, $userkeys) as {} \
								FROM {}".format("name", "name", "email", "email", "emailaddress", "emailaddress", st.session_state["matched_table_name"]))
# Count the records in the encrypted matches table.  Diaplay the time it took and the record count
t_start = datetime.now()
n_records = enc_matches_df.count()

st.write("Overlap matches: {:,}".format(n_records))

# Display sample set of the encrypted matched table
st.dataframe(enc_matches_df.to_pandas())
e_table_name = st.session_state["matched_table_name"] + "_ENCRYPTED"
enc_matches_df.write.mode("overwrite").save_as_table(table_name=e_table_name, table_type='transient')
t_end = datetime.now()
the_delta =  t_end.strptime(t_end.strftime("%H:%M:%S"), "%H:%M:%S") - t_start.strptime(t_start.strftime("%H:%M:%S"), "%H:%M:%S")
t_timing_statement = "Start Time: {} / End Time: {}\n | Total Query Time: {}\n".format(t_start.strftime("%H:%M:%S"), t_end.strftime("%H:%M:%S"), the_delta)
st.write(t_timing_statement)
