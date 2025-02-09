# Distributed Execution of SQL Queries with PrestoDB

## Project Overview
This project benchmarks the performance of **PrestoDB**, a distributed SQL query engine, using three databases: **MongoDB, Cassandra, and PostgreSQL**. The goal is to evaluate PrestoDB's efficiency in handling different query types across heterogeneous data stores under various configurations.

## Prerequisites
Before running the project, ensure that the following dependencies are installed on your system:

- **Python 3.13**
- **Docker**
- **Docker Compose**

## Installation & Setup
1. **Clone the repository**
   ```bash
   git clone https://github.com/pgkouskos/is-project.git
   cd is-project
   ```
2. **Start the services**
   ```bash
   docker-compose up --scale prestodb-worker=<num_of_workers>
   ```
   This command initializes PrestoDB, MongoDB, Cassandra, and PostgreSQL using Docker Compose.

3. **Verify the setup**
   Ensure all containers are running by executing:
   ```bash
   docker ps
   ```

## Data Generation & Loading
To populate the databases with benchmark data, we use the **TPC-DS benchmark**. The necessary scripts for schema creation and data loading are provided in the `/load/` directory.

- **PostgreSQL:**  
  ```bash
  python load/postgres-loader.py
  ```
- **MongoDB:**  
  ```bash
  python load/mongo-loader.py
  ```
- **Cassandra:**  
  Due to Python 3.13 incompatibility with the `cassandra-driver`, data is preprocessed and loaded manually:
  ```bash
  python load/cassandra-preprocessor.py
  cqlsh -f load/cassandra-loader.sql
  ```

## Running Benchmarks
The benchmarking phase consists of executing different SQL queries via PrestoDB:
1. **Run the benchmarking script**
   ```bash
   python /benchmark/scenario-<n>/runner.py
   ```
   This script executes a predefined set of SQL queries and records performance metrics.

## References
- PrestoDB: [https://prestodb.io/](https://prestodb.io/)
- TPC-DS Benchmark: [https://www.tpc.org/tpcds/](https://www.tpc.org/tpcds/)
- Query Set: [https://github.com/agirish/tpcds](https://github.com/agirish/tpcds)


