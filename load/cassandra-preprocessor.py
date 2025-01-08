import os
import json
from dotenv import load_dotenv

load_dotenv()

DATA_DIR = os.getenv("TEST_DATA_LOCAL_PATH")
SCHEMA_DIR = os.getenv("TEST_DATA_SCHEMA_LOCAL_PATH")
TMP_DATA_DIR = os.getenv("TEST_DATA_TMP_LOCAL_PATH")

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

def preprocess_data(file_path, columns):
    """Preprocess data to convert empty values to None and return as list of tuples."""
    data = []
    with open(file_path, 'r') as infile:
        for line in infile:
            cleaned_line = line.rstrip()
            if cleaned_line.endswith('|'):
                cleaned_line = cleaned_line[:-1]
            # Split the line by '|' and replace any empty field with None for Cassandra compatibility
            fields = cleaned_line.split('|')
            fields = [None if field == '' else field for field in fields]
            # Map the fields to the corresponding columns, assuming fields and columns are aligned
            row = dict(zip(columns, fields))
            # Convert the dictionary into a tuple (preserving the order of columns)
            data.append(tuple(row[col] for col in columns))
    return data

def write_preprocessed_data(data, table_name, columns):
    """Write preprocessed data and headers back to a file in the TEST_DATA_TMP_LOCAL_PATH."""
    output_file = os.path.join(TMP_DATA_DIR, f"{table_name}_cassandra.dat")
    with open(output_file, 'w') as outfile:
        # Write the header (columns) first
        outfile.write('|'.join(columns) + '\n')
        # Write the preprocessed data rows
        for row in data:
            # Convert None back to empty string and join with '|'
            line = '|'.join('' if field is None else str(field) for field in row)
            outfile.write(line + '\n')

def main():
    # Preprocess data for each file in the data directory
    for file_name in os.listdir(DATA_DIR):
        if file_name.endswith(".dat"):
            table_name = os.path.splitext(file_name)[0].lower()
            file_path = os.path.join(DATA_DIR, file_name)
            try:
                table_columns = get_columns_from_schema(table_name)
                data = preprocess_data(file_path, table_columns)
                write_preprocessed_data(data, table_name, table_columns)
                print(f"Data for table {table_name} preprocessed and written successfully.")
            except FileNotFoundError as e:
                print(e)
            except Exception as e:
                print(f"Error preprocessing data for table {table_name}: {e}")

if __name__ == "__main__":
    main()
