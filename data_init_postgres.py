import os
import psycopg
from dotenv import load_dotenv

load_dotenv()

DATA_DIR = os.getenv("TEST_DATA_LOCAL_PATH")
HEADERS_DIR = os.getenv("TEST_DATA_HEADER_LOCAL_PATH")

DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": "localhost",
    "port": 5432
}

def preprocess_data(file_path):
    # Define the path for the temporary cleaned file
    temp_file_path = f"{file_path}.tmp"
    with open(file_path, 'r') as infile, open(temp_file_path, 'w') as outfile:
        for line in infile:
            # Strip the line to remove any trailing whitespace, then remove trailing '|'
            cleaned_line = line.rstrip()
            if cleaned_line.endswith('|'):
                cleaned_line = cleaned_line[:-1]
            # Split the line by the delimiter '|' and replace any empty field with '\N' for PostgreSQL compatibility
            fields = cleaned_line.split('|')
            fields = [r'\N' if field == '' else field for field in fields]
            # Rebuild the line by joining the fields with the delimiter '|'
            cleaned_line = '|'.join(fields)
            # Write the cleaned line to the temporary file
            outfile.write(cleaned_line + '\n')
    return temp_file_path

def load_data_to_table(cursor, table_name, file_path):
    temp_file_path = preprocess_data(file_path)
    try:
        print(f"Loading data into {table_name} from {temp_file_path}")
        with open(temp_file_path, "r") as file:
            with cursor.copy(f"COPY {table_name} FROM STDIN WITH DELIMITER '|'") as copy:
                while data := file.read(100):
                    copy.write(data)
        print(f"Data loaded into {table_name} successfully")
    except Exception as e:
        print(f"Error during COPY command for {table_name}: {e}")
    finally:
        os.remove(temp_file_path)

def main():
    conn = psycopg.connect(**DB_CONFIG)
    conn.autocommit = True
    try:
        with conn.cursor() as cursor:
            for file_name in os.listdir(DATA_DIR):
                if file_name.endswith(".dat"):
                    table_name = os.path.splitext(file_name)[0].lower()
                    file_path = os.path.join(DATA_DIR, file_name)
                    try:
                        load_data_to_table(cursor, table_name, file_path)
                    except FileNotFoundError as e:
                        print(e)
                    except Exception as e:
                        print(f"Error loading data into {table_name}: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
