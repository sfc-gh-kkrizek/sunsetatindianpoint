import os
import streamlit as st
import plotly.express as px
import pandas as pd
import snowflake.connector  #upm package(snowflake-connector-python==2.7.0)
import st_connection
import st_connection.snowflake


# Initialize connection, using st.experimental_singleton to only run once.
@st.experimental_singleton(suppress_st_warning=True)
def init_connection():
#    session = snowflake.connector.connect(
#        user=os.getenv("SAIP_USER"),
#        password=os.getenv("SAIP_PASSWORD"),
#        account=os.getenv("ACCOUNT"),
#        role=os.getenv("SAIP_ROLE"),
#        warehouse=os.getenv("SAIP_WAREHOUSE"),
#    )
# Accept Snowflake credentials
   session = st.connection.snowflake.login({'account': 'fba80708',
           'user': 'sunsetatindianpoint',
           'password': None,
           'role': 'dba_saip',
           'warehouse': 'saip_wh_small'
       }, {
           'ttl': 120
       }, 'Snowflake Login')
   return session

st.set_page_config(page_title="Sunset At Indian Point", page_icon="🏞️", layout="centered")

st.title("🏞️ Sunset At Indian Point")

@st.experimental_memo(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()

# Connect to Snowflake
conn = init_connection()

#Query to retrieve source of rental bookings
select = "select sum(r.rental_amt) as rental, src.dim_display as source  from SUNSET.RESERVATIONS.RESERVATIONS r"
join_status = "join SUNSET.RESERVATIONS.bookings_dim st on r.dim_status_id = st.dim_id and st.dim_type = 'STATUS' and st.dim_display = 'Reserved'"
join_type = "join SUNSET.RESERVATIONS.bookings_dim tp on r.type_dim_id = tp.dim_id and tp.dim_type = 'TYPE' and tp.dim_display != 'Family'"
join_source = "join SUNSET.RESERVATIONS.bookings_dim src on r.source_dim_id = src.dim_id and src.dim_type = 'SOURCE'"
group = "group by src.dim_display having sum(r.rental_amt) > 0;"

query = select + " " + join_status + " " + join_type + " " + join_source + " " + group
#use with secure connection
df = conn.sql(query).to_pandas()

#Horizontal bar chart for rental income source
st.write("Rental Income by Source")
fig=px.bar(df,x="RENTAL",y="SOURCE", orientation='h')
st.write(fig)

#Rental Income by Unit
select = "select sum(r.rental_amt) as rental, to_char(r.building_unit) as building from SUNSET.RESERVATIONS.RESERVATIONS r"
group = "group by r.building_unit having sum(r.rental_amt) > 0;"

query = select + " " + join_status + " " + join_type + " " + group
#use with secure connection
df = conn.sql(query).to_pandas()
print(df)

#Horizontal Bar Chart for Income by Unit
st.write("Rental Income by Unit")
fig=px.bar(df,x="RENTAL",y="BUILDING", orientation='h')
st.write(fig)
