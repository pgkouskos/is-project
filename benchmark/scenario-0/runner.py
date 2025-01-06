import os
import json
import psycopg
from dotenv import load_dotenv

load_dotenv()

# Load PostgreSQL credentials and settings
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
RESULTS_SCENARIO_0_LOCAL_PATH = os.path.join(os.getenv("RESULTS_LOCAL_PATH"), 'scenario-0')
QUERIES_LOCAL_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'queries')  # Relevant path to the 'queries' directory

def execute_postgres_query(file_path, output_file):
    """
    Execute a PostgreSQL query from a file and save the Query Latency and Optimizer Plan to a JSON file.
    """
    try:
        # Read the SQL query from the file
        with open(file_path, 'r') as query_file:
            query = query_file.read()

        # Establish a connection to PostgreSQL
        conn = psycopg.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            dbname=POSTGRES_DB
        )
        
        # Create a cursor to interact with the database
        cursor = conn.cursor()

        # Capture the optimizer plan using EXPLAIN command
        cursor.execute(f"EXPLAIN (ANALYZE, VERBOSE) {query}")
        optimizer_plan = cursor.fetchall()

        # Close the cursor and connection
        cursor.close()
        conn.close()

        # Extract the execution time from the last element of the optimizer plan
        optimizer_execution_time_str = optimizer_plan[-1][0]  # Last element
        execution_time_from_plan = None

        # Look for the pattern "Execution Time: X ms" and extract the value
        if "Execution Time:" in optimizer_execution_time_str:
            execution_time_from_plan = optimizer_execution_time_str.split("Execution Time:")[-1].strip().split(" ")[0]

            # Convert ms to seconds (if it is in milliseconds)
            if "ms" in optimizer_execution_time_str:
                execution_time_from_plan = float(execution_time_from_plan) / 1000  # Convert ms to seconds
                execution_time_from_plan = f"{execution_time_from_plan:.6f} s"  # Format as string

        # Prepare the result in a dictionary
        result = {
            "executionTime": execution_time_from_plan,
            "optimizerPlan": optimizer_plan
        }

        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        # Save results to the output file in JSON format
        with open(output_file, 'w', encoding='utf-8') as output:
            json.dump(result, output, indent=4)

        print(f"Query metadata saved to {output_file}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    # Get all .sql files in the 'queries' directory
    sql_files = [f for f in os.listdir(QUERIES_LOCAL_PATH) if f.endswith('.sql')]

    for sql_file in sql_files:
        print(f"Executing {sql_file}")
        # Full path of the .sql file
        sql_file_path = os.path.join(QUERIES_LOCAL_PATH, sql_file)

        # Define the output file path (change extension to .json)
        output_file_path = os.path.join(RESULTS_SCENARIO_0_LOCAL_PATH, os.path.splitext(sql_file)[0] + '.json')

        # Execute the PostgreSQL query and save the result as a JSON file
        execute_postgres_query(sql_file_path, output_file_path)
