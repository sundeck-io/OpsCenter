
CREATE OR REPLACE STREAMLIT admin.opscenter
  FROM '/ui'
  MAIN_FILE = '/Home.py';

GRANT USAGE ON STREAMLIT admin.opscenter TO APPLICATION ROLE ADMIN;
