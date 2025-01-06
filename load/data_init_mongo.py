import os
import json
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

DATA_DIR = os.getenv("TEST_DATA_LOCAL_PATH")
SCHEMA_DIR = os.getenv("TEST_DATA_SCHEMA_LOCAL_PATH")
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = os.getenv("MONGO_DB")

def get_columns_from_schema(table_name):
    """Read column names from the schema JSON file for a given table."""
    schema_file = os.path.join(SCHEMA_DIR, f"{table_name}.json")
    if not os.path.exists(schema_file):
        raise FileNotFoundError(f"Schema file not found for table: {table_name}")
    
    with open(schema_file, "r") as file:
        table_schema = json.load(file)
    
    # Extract column names from the schema
    columns = list(table_schema.keys())
    return columns

def load_table_schema(table_name):
    """Load the schema for a table from a JSON file."""
    schema_file = os.path.join(SCHEMA_DIR, f"{table_name}.json")
    if not os.path.exists(schema_file):
        raise FileNotFoundError(f"Schema file not found for table: {table_name}")
    with open(schema_file, "r") as file:
        return json.load(file)

def convert_type(value, target_type):
    """Convert a string value to a specific type."""
    if value is None:
        return None
    try:
        if target_type == "int":
            return int(value)
        elif target_type == "float":
            return float(value)
        elif target_type == "date":
            # Adjust date format as per your actual data
            return datetime.strptime(value, "%Y-%m-%d")
        else:  # Default to str
            return str(value)
    except ValueError:
        raise ValueError(f"Cannot convert value '{value}' to {target_type}")

def preprocess_data(file_path, table_columns, table_schema):
    """Preprocess data to convert values to appropriate types and return as JSON format."""
    data = []
    with open(file_path, 'r') as infile:
        for line in infile:
            cleaned_line = line.rstrip()
            if cleaned_line.endswith('|'):
                cleaned_line = cleaned_line[:-1]
            fields = cleaned_line.split('|')
            fields = [None if field == '' else field for field in fields]
            # Create a dictionary matching the columns with the data
            document = {}
            for i in range(len(fields)):
                column = table_columns[i]
                value = fields[i]
                target_type = table_schema.get(column, "str")
                document[column] = convert_type(value, target_type)
            data.append(document)
    return data

def load_data_to_mongo(db, collection_name, file_path, table_columns, table_schema):
    """Load data into MongoDB collection."""
    data = preprocess_data(file_path, table_columns, table_schema)
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
                # Get columns from schema
                table_columns = get_columns_from_schema(table_name)
                # Load the schema for the table
                table_schema = load_table_schema(table_name)
                # Load data into the MongoDB collection
                load_data_to_mongo(db, table_name, file_path, table_columns, table_schema)
                print(f"Data loaded into MongoDB collection {table_name} from {file_name}")
            except FileNotFoundError as e:
                print(e)
            except Exception as e:
                print(f"Error loading data into MongoDB collection {table_name}: {e}")

if __name__ == "__main__":
    main()