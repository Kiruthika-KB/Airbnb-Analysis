import pandas as pd
import decimal
import streamlit as st
from pymongo import MongoClient


def connect_to_mongodb(uri, db_name, collection_name):
    """Connect to MongoDB and retrieve the collection."""
    client = MongoClient(uri)
    db = client[db_name]
    collection = db[collection_name]
    return pd.DataFrame(list(collection.find()))


def clean_data(df):
    """Clean and preprocess Airbnb dataset."""
    print("0 - Initial cleaning started.")
    st.write("Starting data cleaning... Please wait.")

    # Identify columns with unhashable data types
    unhashable_columns = [
        col for col in df.columns
        if df[col].apply(lambda x: isinstance(x, (decimal.Decimal, list, dict))).any()
    ]
    st.write(f"Identified unhashable columns: {unhashable_columns}")
    print("1 - Identified unhashable columns:", unhashable_columns)

    # Convert or drop unhashable columns
    for col in unhashable_columns:
        try:
            # Convert Decimal128 to float
            if df[col].apply(lambda x: isinstance(x, decimal.Decimal)).any():
                df[col] = df[col].apply(lambda x: float(x.to_decimal()) if isinstance(x, decimal.Decimal) else x)
            # Handle lists/dictionaries (drop or convert to string if necessary)
            elif df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                df[col] = df[col].astype(str)  # Convert to string
        except Exception as e:
            print(f"Error processing column {col}: {e}. Dropping the column.")
            df.drop(columns=[col], inplace=True)

    # Show progress in Streamlit
    progress = st.progress(0)

    print("2 - Processed unhashable columns.")
    
    # Drop duplicates and handle missing values
    try:
        df.drop_duplicates(inplace=True)
        progress.progress(30)
    except Exception as e:
        print(f"Error during drop_duplicates: {e}.")
    
    df.fillna({"price": "0", "availability_365": "0", "review_scores_rating": 0}, inplace=True)
    progress.progress(60)
    print("3 - Duplicates dropped and missing values filled.")

    # Custom function to handle type conversion
    def convert_to_numeric(value):
        if isinstance(value, decimal.Decimal):
            return float(value.to_decimal())
        elif isinstance(value, str):
            return pd.to_numeric(value.replace("$", "").replace(",", ""), errors="coerce")
        else:
            return pd.to_numeric(value, errors="coerce")

    # Process 'price' column
    if "price" in df.columns:
        try:
            df["price"] = df["price"].apply(convert_to_numeric)
        except Exception as e:
            print(f"Error processing 'price' column: {e}")
    print("4 - Price column processed.")
    progress.progress(80)

    # Process 'availability_365' column
    if "availability_365" in df.columns:
        try:
            df["availability_365"] = df["availability_365"].apply(
                lambda x: float(x.to_decimal()) if isinstance(x, decimal.Decimal) else pd.to_numeric(x, errors="coerce")
            )
        except Exception as e:
            print(f"Error processing 'availability_365' column: {e}")
    print("5 - Availability column processed.")
    progress.progress(100)

    return df


# Main execution
if __name__ == "__main__":
    # MongoDB connection details
    MONGO_URI = "mongodb+srv://user1:Welcome_123@cluster0.rzwxd.mongodb.net/sample_airbnb"  # Replace with your MongoDB Atlas URI
    DB_NAME = "sample_airbnb"  # Replace with your database name
    COLLECTION_NAME = "listingsAndReviews"  # Replace with your collection name

    # Connect to MongoDB and retrieve data
    st.write("Connecting to MongoDB... This may take a moment.")
    raw_data = connect_to_mongodb(MONGO_URI, DB_NAME, COLLECTION_NAME)

    # Sample a small part of the data for quicker visualization
    raw_data = raw_data.sample(n=100)  # Sample 100 rows, change as needed

    st.write("Cleaning data...")
    cleaned_df = clean_data(raw_data)

    # Display cleaned data as a table
    st.write("Data cleaning complete. Displaying the cleaned data:")
    st.dataframe(cleaned_df.head())  # Display first few rows

    # Save cleaned data or proceed with further analysis
    st.write("Saving cleaned data to CSV...")
    cleaned_df.to_csv("cleaned_airbnb_data.csv", index=False)

    st.write("Cleaned data saved. Ready for further analysis!")
