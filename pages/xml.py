import streamlit as st
import pandas as pd
import polars as pl
from pymongo import MongoClient
from bson.objectid import ObjectId
import urllib.parse
import io  # Added the missing import

# Define the selected columns
selected_columns = [
    "_id",
    "Purchasing Document",
    "Supplier Name",
    "Order Date",
    "Total Amount",
    "codigo_projeto",
    "Project Code"
    # Add other columns as needed
]

# Function to convert ObjectId to strings and rename "_id" to "ObjectId"
def convert_objectid_to_str(documents):
    for document in documents:
        if "_id" in document:
            document["ObjectId"] = str(document.pop("_id"))
        for key, value in document.items():
            if isinstance(value, ObjectId):
                document[key] = str(value)
    return documents

# Function to load the entire collection without grouping
def mongo_collection_to_polars_without_grouping(mongo_uri, db_name, collection_name, selected_columns):
    with MongoClient(mongo_uri) as client:
        db = client[db_name]
        collection = db[collection_name]
        
        projection = {col: 1 for col in selected_columns}
        documents = list(collection.find({}, projection))
        
        # Convert ObjectId to strings
        documents = convert_objectid_to_str(documents)
        
        # Create Polars DataFrame
        if not documents:
            return pl.DataFrame()
        
        try:
            polars_df = pl.DataFrame(documents, infer_schema_length=1000)
        except Exception as e:
            st.error(f"Error creating Polars DataFrame: {e}")
            return pl.DataFrame()
        
        return polars_df

# MongoDB connection parameters
username = st.secrets["MONGO_USERNAME"]
password = st.secrets["MONGO_PASSWORD"]
cluster = st.secrets["MONGO_CLUSTER"]
db_name = st.secrets["MONGO_DB"]
collection_name = 'po'

# Escapar o nome de usuário e a senha
escaped_username = urllib.parse.quote_plus(username)
escaped_password = urllib.parse.quote_plus(password)

# Montar a string de conexão
MONGO_URI = f"mongodb+srv://{escaped_username}:{escaped_password}@{cluster}/{db_name}?retryWrites=true&w=majority"

# Load data into Polars DataFrame
polars_df = mongo_collection_to_polars_without_grouping(
    MONGO_URI, db_name, collection_name, selected_columns
)

# Convert Polars DataFrame to Pandas DataFrame
if not polars_df.is_empty():
    pandas_df = polars_df.to_pandas()
else:
    pandas_df = pd.DataFrame()

# Display data in Streamlit
if not pandas_df.empty:
    st.dataframe(pandas_df)
else:
    st.warning("No data available to display.")

# Offer download button for Excel file
if not pandas_df.empty:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        pandas_df.to_excel(writer, index=False, sheet_name='Data')
    
    st.download_button(
        label="Download Excel File",
        data=buffer.getvalue(),
        file_name='data.xlsx',
        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
else:
    st.warning("No data to download.")