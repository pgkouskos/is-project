# Distributed SQL Query Execution with PrestoDB

## Project Overview
This project benchmarks the performance of **PrestoDB**, a distributed SQL query engine, across **MongoDB, Cassandra, and PostgreSQL**. The study involves deploying these databases, loading structured data, executing queries, and measuring performance under different execution strategies.

## Prerequisites
Before running the application, ensure you have the following installed:
- **Python 3.13**
- **Docker**
- **Docker Compose**

## Installation and Setup
1. Clone the repository:
   ```sh
   git clone https://github.com/pgkouskos/is-project.git
   cd is-project
   ```

2. Create a **`.env`** file in the root directory with the following content (adjust paths and credentials as needed for your system):
   ```env
   CASSANDRA_DATA_LOCAL_PATH=<path_to_cassandra_data>
   MONGO_DATA_LOCAL_PATH=<path_to_mongo_data>
   MONGO_DB=<mongo_database_name>
   MONGO_HOST=<mongo_host>
   MONGO_PORT=<mongo_port>
   POSTGRES_DATA_LOCAL_PATH=<path_to_postgres_data>
   POSTGRES_DB=<postgres_database_name>
   POSTGRES_USER=<postgres_user>
   POSTGRES_PASSWORD=<postgres_password>
   PRESTO_CATALOG_LOCAL_PATH=<path_to_presto_catalog>
   PRESTO_CONFIG_LOCAL_PATH=<path_to_presto_config>
   PRESTO_JVM_CONFIG_LOCAL_PATH=<path_to_presto_jvm_config>
   PRESTO_HOST=<presto_host>
   PRESTO_PORT=<presto_port>
   PRESTO_USER=<presto_user>
   RESULTS_LOCAL_PATH=<path_to_results>
   TEST_DATA_LOCAL_PATH=<path_to_test_data>
   TEST_DATA_SCHEMA_LOCAL_PATH=<path_to_test_data_schema>
   TEST_DATA_TMP_LOCAL_PATH=<path_to_test_data_tmp>
   QUERIES_LOCAL_PATH=<path_to_queries>
   ```

3. Start the services using Docker Compose:
   ```sh
   docker-compose up --scale prestodb-worker=<num_of_workers>
   ```

## Data Generation and Loading
- The **TPC-DS benchmark** dataset is used for testing.
- Data is loaded into each database using dedicated scripts:
  ```sh
  python load/postgres-loader.py
  python load/mongo-loader.py
  python load/cassandra-preprocessor.py
  python load/cassandra-loader.sql
  ```

## Running Benchmarks
To execute the benchmarking tests, use:
```sh
python benchmark/scenario-<n>/runner.py
```

## Execution Strategies
The benchmarking process follows these phases:
1. **Baseline Performance:** Queries executed on PostgreSQL alone.
2. **Distributed Table Allocation:** Data distributed among MongoDB, Cassandra, and PostgreSQL.
3. **Data Locality Considerations:** Optimizing placement of frequently joined tables.
4. **Scaling Workers:** Performance evaluated using different numbers of PrestoDB workers.

## Results
- Query latency and execution plans are analyzed for different configurations.

## References
- **PrestoDB**: [https://prestodb.io/](https://prestodb.io/)
- **TPC-DS Benchmark**: [https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp)
- **Query Set Used**: [https://github.com/agirish/tpcds](https://github.com/agirish/tpcds)

For detailed findings, refer to the full project report.