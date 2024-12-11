import streamlit as st
import pandas as pd
import polars as pl
import plotly.express as px
import plotly.graph_objects as go
from pymongo import MongoClient
import urllib.parse
from bson.objectid import ObjectId

def convert_objectid_to_str(documents):
    """Convert ObjectId fields to strings in MongoDB documents."""
    for document in documents:
        for key, value in document.items():
            if isinstance(value, ObjectId):
                document[key] = str(value)
    return documents

@st.cache_data
def load_mongodb_collection(mongo_uri, db_name, collection_name):
    """
    Load a MongoDB collection into a Polars DataFrame.
    
    Args:
        mongo_uri (str): MongoDB connection URI
        db_name (str): Database name
        collection_name (str): Collection to load
    
    Returns:
        pl.DataFrame: Loaded collection data
    """
    # Connect to MongoDB
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    # Retrieve all documents
    documents = list(collection.find())

    # Convert ObjectId to strings
    documents = convert_objectid_to_str(documents)

    # Handle empty result
    if not documents:
        return pl.DataFrame()

    # Convert to Polars DataFrame
    try:
        return pl.DataFrame(documents, infer_schema_length=1000)
    except Exception as e:
        st.error(f"Error creating Polars DataFrame: {e}")
        return pl.DataFrame()

def generate_charts(df):
    """
    Generate interactive charts based on user selections
    """
    # Ensure DataFrame is not empty
    if df.is_empty():
        st.warning("No data to visualize")
        return

    # Convert Polars DataFrame to Pandas for plotting
    pandas_df = df.to_pandas()

    # Chart type selection
    st.header("📊 Data Visualization")
    
    # Columns selection
    st.subheader("Select Columns for Analysis")
    
    # Select columns for x-axis and y-axis
    numeric_columns = list(pandas_df.select_dtypes(include=['int64', 'float64']).columns)
    categorical_columns = list(pandas_df.select_dtypes(include=['object']).columns)
    
    # Column selection for grouping and analysis
    group_column = st.selectbox("Select Column for Grouping", 
                                 categorical_columns + ['None'], 
                                 key='group_column')
    
    x_column = st.selectbox("Select X-Axis Column", 
                             categorical_columns + numeric_columns, 
                             key='x_column')
    
    y_column = st.selectbox("Select Y-Axis Column (Numeric)", 
                             numeric_columns, 
                             key='y_column')
    
    # Chart type selection
    chart_type = st.selectbox("Select Chart Type", 
        ['Bar Chart', 'Pie Chart', 'Scatter Plot', 'Box Plot', 'Histogram'], 
        key='chart_type')
    
    # Aggregation method for numeric columns
    agg_method = st.selectbox("Aggregation Method", 
        ['Count', 'Sum', 'Mean', 'Median', 'Max', 'Min'], 
        key='agg_method')
    
    # Generate chart based on selections
    if group_column != 'None' and chart_type == 'Bar Chart':
        if agg_method == 'Count':
            grouped_data = pandas_df.groupby(group_column)[y_column].count().reset_index()
        elif agg_method == 'Sum':
            grouped_data = pandas_df.groupby(group_column)[y_column].sum().reset_index()
        elif agg_method == 'Mean':
            grouped_data = pandas_df.groupby(group_column)[y_column].mean().reset_index()
        elif agg_method == 'Median':
            grouped_data = pandas_df.groupby(group_column)[y_column].median().reset_index()
        elif agg_method == 'Max':
            grouped_data = pandas_df.groupby(group_column)[y_column].max().reset_index()
        elif agg_method == 'Min':
            grouped_data = pandas_df.groupby(group_column)[y_column].min().reset_index()
        
        fig = px.bar(grouped_data, x=group_column, y=y_column, 
                     title=f'{agg_method} of {y_column} by {group_column}')
    
    elif chart_type == 'Pie Chart' and group_column != 'None':
        grouped_data = pandas_df.groupby(group_column)[y_column].sum().reset_index()
        fig = px.pie(grouped_data, values=y_column, names=group_column, 
                     title=f'Distribution of {y_column} by {group_column}')
    
    elif chart_type == 'Scatter Plot':
        fig = px.scatter(pandas_df, x=x_column, y=y_column, 
                         color=group_column if group_column != 'None' else None,
                         title=f'Scatter Plot of {x_column} vs {y_column}')
    
    elif chart_type == 'Box Plot':
        if group_column != 'None':
            fig = px.box(pandas_df, x=group_column, y=y_column, 
                         title=f'Box Plot of {y_column} by {group_column}')
        else:
            fig = px.box(pandas_df, y=y_column, 
                         title=f'Box Plot of {y_column}')
    
    elif chart_type == 'Histogram':
        fig = px.histogram(pandas_df, x=x_column, color=group_column if group_column != 'None' else None,
                           title=f'Histogram of {x_column}')
    
    else:
        st.warning("Please select appropriate columns for the chart")
        return
    
    # Display the chart
    st.plotly_chart(fig, use_container_width=True)

def mongodb_collection_loader():
    """
    Streamlit app for loading and displaying MongoDB collections with flexible charting
    """
    st.header("🗂️ MongoDB Collection Explorer")

    # MongoDB Connection Details
    st.expander("MongoDB Connection", expanded=False)
    username = st.text_input("Username", value="devpython86")
    password = st.text_input("Password", type="password", value="dD@pjl06081420")
    cluster = st.text_input("Cluster", value="cluster0.akbb8.mongodb.net")
    db_name = st.text_input("Database Name", value="warehouse")

    # Create MongoDB URI
    escaped_username = urllib.parse.quote_plus(username)
    escaped_password = urllib.parse.quote_plus(password)
    MONGO_URI = f"mongodb+srv://{escaped_username}:{escaped_password}@{cluster}/{db_name}?retryWrites=true&w=majority"

    # Get available collections
    try:
        client = MongoClient(MONGO_URI)
        collections = list(client[db_name].list_collection_names())
    except Exception as e:
        st.error(f"Error connecting to MongoDB: {e}")
        collections = []

    # Collection selection
    selected_collection = st.selectbox("Select Collection", collections)

    # Load collection button
    if st.button("Load Collection"):
        with st.spinner("Carregando dados..."):
            # Load the selected collection
            df = load_mongodb_collection(MONGO_URI, db_name, selected_collection)
            
            if not df.is_empty():
                # Display raw data
                st.subheader("Raw Data Preview")
                st.dataframe(df.to_pandas())
                
                # Generate charts
                generate_charts(df)
            else:
                st.warning("No data found in the collection")

if __name__ == "__main__":
    mongodb_collection_loader()