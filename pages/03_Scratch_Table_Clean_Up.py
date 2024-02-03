import pandas as pd
from io import StringIO
import streamlit as st
from streamlit.logger import get_logger
from dotenv import load_dotenv
import os
import logging
from datetime import datetime, timedelta
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col


st.set_page_config(page_title="Scratch Table Clean Up")

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

d_table_name = st.session_state["d_table_name"]
m_table_name = st.session_state["matched_table_name"]
tmp_match_table = st.session_state["tmp_match_table"]

m_session1 = Session.builder.configs(auth_info).create()

drop_d_table = m_session1.sql("DROP TABLE {}".format(d_table_name)).collect()

st.write(drop_d_table)

drop_matched_table = m_session1.sql("DROP TABLE {}".format(m_table_name)).collect()

st.write(drop_matched_table)

drop_tmp_match_table = m_session1.sql("DROP TABLE {}".format(tmp_match_table))

st.write(drop_tmp_match_table)
