import streamlit as st
import pymssql
import pandas as pd
from sqlalchemy import create_engine
import time
import plotly.express as px

# SQL server settings
sql_server = "iotserveranket.database.windows.net"
sql_user = "admin_anket"
sql_password = "Amazing$888"
sql_database = "Iot"

# Function to fetch data from SQL server based on date range
def fetch_data(start_date, end_date, aggregate_by_hour):
    try:
        # Create SQLAlchemy engine
        engine = create_engine(f"mssql+pymssql://{sql_user}:{sql_password}@{sql_server}/{sql_database}")

        # Convert dates to strings
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')

        # Adjust end_date to include the entire day
        end_date_adjusted = end_date + pd.Timedelta(days=1)

        if aggregate_by_hour:
            # Formulate the SQL query to fetch hourly averages
            query = f"""
            SELECT 
                DATEADD(HOUR, DATEDIFF(HOUR, 0, timestamp), 0) AS timestamp_hour,
                AVG(temperature) AS avg_temperature,
                AVG(humidity) AS avg_humidity
            FROM temperature_data
            WHERE timestamp BETWEEN '{start_date_str}' AND '{end_date_adjusted}'
            GROUP BY DATEADD(HOUR, DATEDIFF(HOUR, 0, timestamp), 0)
            """
        else:
            # Formulate the SQL query to fetch raw data
            query = f"SELECT timestamp, temperature, humidity FROM temperature_data WHERE timestamp BETWEEN '{start_date_str}' AND '{end_date_adjusted}'"

        # Fetch data into DataFrame
        df = pd.read_sql(query, engine)

        return df
    except Exception as e:
        st.error("Error fetching data: {}".format(str(e)))
        return None

# Streamlit app
def main():
    st.title("Temperature and Humidity Data")

    # Sidebar for filter options
    st.sidebar.title("Filter Options")
    start_date = st.sidebar.date_input("Start Date")
    end_date = st.sidebar.date_input("End Date")

    # Date selection
    if start_date <= end_date:
        st.write("### Displaying data from {} to {}".format(start_date, end_date))
        aggregate_by_hour = st.sidebar.checkbox("Average by Hour")

        df = fetch_data(start_date, end_date, aggregate_by_hour)
        if df is not None and not df.empty:  # Check if df is not None and not empty
            if aggregate_by_hour:
                st.write("### Hourly Average Temperature and Humidity Data")
                st.write(df)

                # Create bar chart
                st.write("### Hourly Average Temperature and Humidity Bar Chart")
                fig = px.bar(df, x='timestamp_hour', y=['avg_temperature', 'avg_humidity'], barmode='group', labels={'timestamp_hour': 'Time', 'value': 'Value', 'variable': 'Measurement'})
                st.plotly_chart(fig)
            else:
                st.write("### Raw Temperature and Humidity Data")
                st.write(df)

            # Real-time line chart
            st.write("### Real-time Temperature and Humidity Data")
            chart = st.line_chart()

            # Real-time pie chart
            st.write("### Real-time Pie Chart for Temperature and Humidity")
            pie_chart = st.empty()

            # Continuously update the line chart every 15 seconds
            while True:
                df = fetch_data(start_date, end_date, aggregate_by_hour)
                if df is not None and not df.empty:  # Check if df is not None and not empty
                    if aggregate_by_hour:
                        chart.line_chart(df.set_index('timestamp_hour')[['avg_temperature', 'avg_humidity']])
                    else:
                        chart.line_chart(df.set_index('timestamp'))

                    # Update pie chart
                    pie_df = df.mean().reset_index()
                    pie_df.columns = ['measurement', 'value']
                    fig = px.pie(pie_df, values='value', names='measurement', title='Average Temperature and Humidity')
                    pie_chart.plotly_chart(fig)

                time.sleep(15)  # Wait for 15 seconds before fetching data again
        else:
            st.error("No data available for the selected date range.")
    else:
        st.error("End date must be after start date.")

if __name__ == "__main__":
    main()
