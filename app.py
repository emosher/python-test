import numpy as np
import pandas as pd
import streamlit as st
import psycopg2
from pyservicebinding import binding

st.title("Uber pickups in NYC")
st.subheader("Built using Tanzu Application Platform v1.3.2 - AppTest")

DATE_COLUMN = "date/time"
DATA_URL = (
    "https://s3-us-west-2.amazonaws.com/"
    "streamlit-demo-data/uber-raw-data-sep14.csv.gz"
)

@st.cache
def load_data(nrows):
    data = pd.read_csv(DATA_URL, nrows=nrows)
    lowercase = lambda x: str(x).lower()
    data.rename(lowercase, axis="columns", inplace=True)
    data[DATE_COLUMN] = pd.to_datetime(data[DATE_COLUMN])
    return data

def seed_database():
    try:
        sb = binding.ServiceBinding()
        db_binding = sb.bindings("psql")
        conn = psycopg2.connect(
            user=db_binding["user"],
            password=db_binding["password"],
            host=db_binding["host"],
            port=db_binding["port"],
            database=db_binding["database"],
        )
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM uber;")
        result = cursor.fetchone()
        user_count = result[0]

        if user_count == 0:
            data = load_data(10000)

            create_table_query = """
            CREATE TABLE IF NOT EXISTS uber (
                -- Define your table columns here
            );
            """
            cursor.execute(create_table_query)

            insert_query = """
            INSERT INTO uber (column1, column2, ...)
            VALUES (%s, %s, ...);
            """

            for _, row in data.iterrows():
                values = tuple(row[column] for column in data.columns)  # Get values from all columns
                cursor.execute(insert_query, values)

        conn.commit()
        cursor.close()
        conn.close()
        return data
    except Exception as e:
        st.error("Error seeding the database: " + str(e))
        return None

data = seed_database()

if data:
    if st.checkbox("Show raw data"):
        st.subheader("Raw data")
        st.write(data)

    st.subheader("Number of pickups by hour")
    hist_values = np.histogram(data[DATE_COLUMN].dt.hour, bins=24, range=(0, 24))[0]
    st.bar_chart(hist_values)

    hour_to_filter = st.slider("hour", 0, 23, 17)
    filtered_data = data[data[DATE_COLUMN].dt.hour == hour_to_filter]

    st.subheader("Map of all pickups at %s:00" % hour_to_filter)
    st.map(filtered_data)
