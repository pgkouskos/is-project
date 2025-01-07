import requests
import os
import time
from dotenv import load_dotenv
import json

load_dotenv()

PRESTO_HOST = os.getenv("PRESTO_HOST")
PRESTO_PORT = os.getenv("PRESTO_PORT")
PRESTO_USER = os.getenv("PRESTO_USER")
RESULTS_SCENARIO_1_LOCAL_PATH = os.path.join(os.getenv("RESULTS_LOCAL_PATH"), 'scenario-1')
QUERIES_LOCAL_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'queries')  # Relevant path to the 'queries' directory

def execute_presto_query(file_path, output_file):
    """
    Execute a Presto query from a file and save the Query Latency and Optimizer Plan to a JSON file.
    """
    try:
        # Read the SQL query from the file
        with open(file_path, 'r') as query_file:
            query = query_file.read()

        # Presto server details
        url = f"http://{PRESTO_HOST}:{PRESTO_PORT}/v1/statement"

        # Headers for the Presto request
        headers = {
            'X-Presto-User': PRESTO_USER,
        }

        # Send the query to Presto
        response = requests.post(url, data=query, headers=headers)

        # Check for a successful response
        if response.status_code != 200:
            raise Exception(f"Presto query failed: {response.text}")

        # Extract query tracking URL
        response_json = response.json()
        next_uri = response_json.get('nextUri')
        if not next_uri:
            raise Exception("Failed to retrieve nextUri from Presto response.")

        # Poll for query status using nextUri
        while next_uri:
            query_info_response = requests.get(next_uri)
            
            # Check if the response is valid
            if query_info_response.status_code != 200:
                raise Exception(f"Failed to retrieve query details: {query_info_response.text}")

            # Parse query details
            query_info = query_info_response.json()

            # Check if the query is done
            next_uri = query_info.get('nextUri')
            state = query_info.get('stats', {}).get('state', 'UNKNOWN')
            print(f"Query State: {state}")

            if state in {"FINISHED", "FAILED"}:
                break

            # Wait before polling again
            time.sleep(5)

        # Check for failure
        if state == "FAILED":
            error_message = query_info.get('error', {}).get('message', 'Unknown error')
            raise Exception(f"Query failed: {error_message}")
        
        # Get the query execution details
        query_response = requests.get(f"http://{PRESTO_HOST}:{PRESTO_PORT}/v1/query/{query_info.get('id')}").json()
        optimizer_plan_data = query_response.get('optimizerInformation', [])

        # Extract optimizer names without sorting, and remove duplicates while maintaining order
        seen = set()
        optimizer_names = []
        for entry in optimizer_plan_data:
            optimizer_name = entry['optimizerName']
            if optimizer_name not in seen:
                seen.add(optimizer_name)
                optimizer_names.append(optimizer_name)

        # Prepare the JSON result
        result = {
            "executionTime": query_response.get('queryStats').get('executionTime'),
            "optimizerPlan": optimizer_names
        }

        # Save results to the output file in JSON format
        os.makedirs(os.path.dirname(output_file), exist_ok=True)  # Ensure output directory exists
        with open(output_file, 'w', encoding='utf-8') as output:
            json.dump(result, output, indent=4)

        print(f"Query metadata saved to {output_file}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    # Get all .sql files in the directory
    sql_files = [f for f in os.listdir(QUERIES_LOCAL_PATH) if f.endswith('.sql')]

    for sql_file in sql_files:
        print(f"Executing {sql_file}")
        # Full path of the .sql file
        sql_file_path = os.path.join(QUERIES_LOCAL_PATH, sql_file)

        # Define the output file path (change extension to .json)
        output_file_path = os.path.join(RESULTS_SCENARIO_1_LOCAL_PATH, os.path.splitext(sql_file)[0] + '.json')

        # Execute the Presto query and save the result as a JSON file
        execute_presto_query(sql_file_path, output_file_path)