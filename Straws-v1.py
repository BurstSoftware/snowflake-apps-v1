import streamlit as st
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# Streamlit app title
st.title("Snowflake Data App with Streamlit")

# Snowflake connection parameters
@st.cache_resource
def get_snowflake_connection():
    return snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        account=st.secrets["snowflake"]["account"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"]
    )

# Function to load data from Snowflake
@st.cache_data
def load_data(query):
    conn = get_snowflake_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Function to upload DataFrame to Snowflake
def upload_data(df, table_name):
    conn = get_snowflake_connection()
    success, nchunks, nrows, _ = write_pandas(conn, df, table_name)
    conn.close()
    return success, nrows

# Sidebar for user input
st.sidebar.header("Query Options")
query = st.sidebar.text_input("Enter SQL Query", "SELECT * FROM your_table LIMIT 10")

# Load and display data
if st.button("Load Data"):
    try:
        df = load_data(query)
        st.write("### Data from Snowflake")
        st.dataframe(df)
        
        # Basic data statistics
        st.write("### Data Statistics")
        st.write(df.describe())
        
        # Option to download as CSV
        csv = df.to_csv(index=False)
        st.download_button(
            label="Download data as CSV",
            data=csv,
            file_name="snowflake_data.csv",
            mime="text/csv"
        )
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")

# Upload data section
st.header("Upload Data to Snowflake")
uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

if uploaded_file is not None:
    # Read uploaded CSV
    df_upload = pd.read_csv(uploaded_file)
    st.write("### Uploaded Data Preview")
    st.dataframe(df_upload.head())
    
    # Input for table name
    table_name = st.text_input("Enter Snowflake table name for upload")
    
    if st.button("Upload to Snowflake"):
        if table_name:
            try:
                success, nrows = upload_data(df_upload, table_name.upper())
                if success:
                    st.success(f"Successfully uploaded {nrows} rows to {table_name}")
                else:
                    st.error("Upload failed")
            except Exception as e:
                st.error(f"Error uploading data: {str(e)}")
        else:
            st.warning("Please enter a table name")

# Add some basic instructions
st.markdown("""
### Instructions
1. Enter a SQL query in the sidebar to fetch data from Snowflake
2. Click 'Load Data' to display the results
3. Upload a CSV file and specify a table name to push data to Snowflake
4. Ensure your Snowflake credentials are properly configured in Streamlit secrets
""")
