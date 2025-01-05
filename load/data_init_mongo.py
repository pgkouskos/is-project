import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

DATA_DIR = os.getenv("TEST_DATA_LOCAL_PATH")
HEADERS_DIR = os.getenv("TEST_DATA_HEADER_LOCAL_PATH")
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = os.getenv("MONGO_DB")

def get_columns_from_header(table_name):
    """Read column headers from the header file for a given table, splitting by '|'."""
    header_file = os.path.join(HEADERS_DIR, f"{table_name}_header.dat")
    if not os.path.exists(header_file):
        raise FileNotFoundError(f"Header file not found for table: {table_name}")
    with open(header_file, "r") as file:
        # Read columns from the header, split by '|' and strip any extra spaces
        columns = [col.strip() for line in file for col in line.strip().split('|') if col.strip()]
    return columns

def preprocess_data(file_path, table_columns):
    """Preprocess data to convert empty values to 'NULL' (or None in MongoDB) and return as JSON format."""
    data = []
    with open(file_path, 'r') as infile:
        for line in infile:
            cleaned_line = line.rstrip()
            if cleaned_line.endswith('|'):
                cleaned_line = cleaned_line[:-1]
            # Split the line by '|' and replace any empty field with None (MongoDB equivalent of NULL)
            fields = cleaned_line.split('|')
            fields = [None if field == '' else field for field in fields]
            # Create a dictionary matching the columns with the data
            document = {table_columns[i]: fields[i] for i in range(len(fields))}
            data.append(document)
    return data

def load_data_to_mongo(db, collection_name, file_path, table_columns):
    """Load data into MongoDB collection."""
    data = preprocess_data(file_path, table_columns)
    # Insert documents into MongoDB collection
    collection = db[collection_name]
    collection.insert_many(data)

def main():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    # Load data into MongoDB
    for file_name in os.listdir(DATA_DIR):
        if file_name.endswith(".dat"):
            table_name = os.path.splitext(file_name)[0].lower()
            file_path = os.path.join(DATA_DIR, file_name)
            try:
                # Get columns from header
                table_columns = get_columns_from_header(table_name)
                # Load data into the MongoDB collection
                load_data_to_mongo(db, table_name, file_path, table_columns)
                print(f"Data loaded into MongoDB collection {table_name} from {file_name}")
            except FileNotFoundError as e:
                print(e)
            except Exception as e:
                print(f"Error loading data into MongoDB collection {table_name}: {e}")

if __name__ == "__main__":
    main()
