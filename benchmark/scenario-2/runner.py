import os
import json
import time
import requests
import matplotlib.pyplot as plt
from dotenv import load_dotenv

load_dotenv()

# Load Presto credentials and settings
PRESTO_HOST = os.getenv("PRESTO_HOST")
PRESTO_PORT = os.getenv("PRESTO_PORT")
PRESTO_USER = os.getenv("PRESTO_USER")
RESULTS_SCENARIO_2_LOCAL_PATH = os.path.join(os.getenv("RESULTS_LOCAL_PATH"), 'scenario-2')
QUERIES_LOCAL_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'queries')

def execute_presto_query(file_path, output_file):
    """Execute a Presto query, track execution, and save query metadata."""
    try:
        with open(file_path, 'r') as query_file:
            query = query_file.read()

        url = f"http://{PRESTO_HOST}:{PRESTO_PORT}/v1/statement"
        headers = {'X-Presto-User': PRESTO_USER}

        with requests.Session() as session:
            response = session.post(url, data=query, headers=headers)
            if response.status_code != 200:
                raise Exception(f"Presto query failed: {response.text}")

            response_json = response.json()
            next_uri = response_json.get('nextUri')
            if not next_uri:
                raise Exception("Failed to retrieve nextUri from Presto response.")

            # Polling for query status
            while next_uri:
                query_info_response = session.get(next_uri)
                if query_info_response.status_code != 200:
                    raise Exception(f"Failed to retrieve query details: {query_info_response.text}")

                query_info = query_info_response.json()
                next_uri = query_info.get('nextUri')
                state = query_info.get('stats', {}).get('state', 'UNKNOWN')
                print(f"Query State: {state}")

                if state in {"FINISHED", "FAILED"}:
                    break

                time.sleep(5)

            if state == "FAILED":
                error_message = query_info.get('error', {}).get('message', 'Unknown error')
                raise Exception(f"Query failed: {error_message}")

            # Retrieve execution details
            query_id = query_info.get('id')
            query_details_url = f"http://{PRESTO_HOST}:{PRESTO_PORT}/v1/query/{query_id}"
            query_response = session.get(query_details_url).json()

            execution_time = query_response.get('queryStats', {}).get('executionTime')
            if execution_time and execution_time.endswith('s'):
                execution_time_seconds = float(execution_time.replace("s", ""))
            else:
                execution_time_seconds = None

            optimizer_plan_data = query_response.get('optimizerInformation', [])
            optimizer_names = list({entry['optimizerName'] for entry in optimizer_plan_data})

            # Save results
            result = {"executionTime": execution_time_seconds, "optimizerPlan": optimizer_names}
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            with open(output_file, 'w', encoding='utf-8') as output:
                json.dump(result, output, indent=4)

            print(f"Query metadata saved to {output_file}")

    except Exception as e:
        print(f"Error: {e}")

def generate_chart():
    """Reads JSON result files and saves query execution latency as a bar chart."""
    query_times = {}

    for file in os.listdir(RESULTS_SCENARIO_2_LOCAL_PATH):
        if file.endswith(".json"):
            file_path = os.path.join(RESULTS_SCENARIO_2_LOCAL_PATH, file)
            with open(file_path, 'r') as f:
                data = json.load(f)
                execution_time = data.get("executionTime")
                if execution_time is not None:
                    try:
                        execution_time = float(execution_time)
                        query_times[file.replace(".json", "")] = execution_time
                    except ValueError:
                        print(f"Skipping {file} due to invalid execution time format: {execution_time}")

    if query_times:
        plt.figure(figsize=(10, 5))
        plt.bar(query_times.keys(), query_times.values(), color='salmon')
        plt.xlabel("SQL File Name")
        plt.ylabel("Query Latency (seconds)")
        plt.title("Presto Query Execution Times")
        plt.xticks(rotation=45, ha="right")
        plt.grid(axis='y', linestyle='--', alpha=0.7)

        # Ensure the output directory exists
        os.makedirs(RESULTS_SCENARIO_2_LOCAL_PATH, exist_ok=True)

        # Save the figure
        chart_path = os.path.join(RESULTS_SCENARIO_2_LOCAL_PATH, "query_latency_chart.png")
        plt.savefig(chart_path, bbox_inches='tight')
        print(f"Query latency chart saved to {chart_path}")
        
        plt.close()
    else:
        print("No valid execution times found to plot.")

if __name__ == "__main__":
    sql_files = [f for f in os.listdir(QUERIES_LOCAL_PATH) if f.endswith('.sql')]

    for sql_file in sql_files:
        print(f"Executing {sql_file}")
        sql_file_path = os.path.join(QUERIES_LOCAL_PATH, sql_file)
        output_file_path = os.path.join(RESULTS_SCENARIO_2_LOCAL_PATH, os.path.splitext(sql_file)[0] + '.json')
        execute_presto_query(sql_file_path, output_file_path)

    generate_chart()