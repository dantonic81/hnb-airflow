# ETL Application

This ETL (Extract, Transform, Load) application is designed to process customer, product, transaction, and erasure datasets from a local file system, applying data cleanup and anonymization as specified in the challenge. The processed datasets are then stored at a different location.
Orchestration tool of choice is Airflow.



## Project Structure

- `dags/`: Directory containing all needed files including schemas and etl modules
- `sql-scripts/`: Directory containing the script to initialize the database
- `tests/`: Directory containing tests for Python code (not included)

.env.example file contains environment variables added here for convenience.

Airflow DAGs:
- transactions_etl_dag.py
- products_etl_dag.py
- customers_etl_dag.py
- erasure_requests_etl_dag.py

ETL Python modules:
- transactions_etl.py
- products_etl.py
- customers_etl.py
- erasure_requests_etl.py
- common.py: Python utils and commonly shared functions

Schemas (JSON):
- transactions_schema.json
- products_schema.json
- customers_schema.json
- erasure_requests_schema.json

Airflow deployment scripts

- `docker-compose.yml` - main deployment script 


## Prerequisites

- Docker
- Docker Compose
- Python

## Installation

1. Clone the repo
2. Navigate to the project directory
    ```bash
    cd path-to-your-cloned-repo
    ```
   
3. Build and run the Docker-compose project:

    ```bash
    docker compose up --build
    ```

4. Once all containers are up, you will be able to access Airflow webserver at http://0.0.0.0:8080
from where you can run any of the four DAGS. 


5. If you don't want to use Airflow to run DAGs, you can run the etl process manually by exec'ing into the airflow worker container:

    ```bash
    docker exec -it airflow-worker /bin/bash   
    ```
   
    and then running any of the four Python ETL modules e.g.

    ```bash
    python erasure_requests_etl.py 
    ```


6. Job schedules are set as follows but are not turned on for better observability:

    - customers_job (Every hour at minute 0)
    - transactions_job (Every hour at minute 0)
    - products_job (Once a day at 00:00)
    - erasure_job (Once a day at 01:00)


7. PostgreSQL database is accessible by using:

    ```bash
    docker compose exec postgres psql -U airflow -d airflow
    ```

## PROCESSED_DATA and ARCHIVED_DATA folders

Job runs take data from `raw_data` folder and process it into `processed_data` folder.
After processing is done, files are archived to `archived_data` folder and the original files are deleted from `raw_data` folder.


## Configuration

- PostgreSQL database configuration is specified in the `.env` file.

## Testing

You may want to have a separate virtual environment set up before installing dependencies and running tests.
Setting up a virtual environment is outside the scope of this document.

Once in your virtual environment, install dependencies from requirements.txt file:

    pip install -r requirements.txt

All the requirements in requirements.txt file are there only for the purpose of testing, they are not needed
for the program to function because the Airflow deployment already comes with them included. 

Run the unit tests (not included) using pytest:

   ```bash
   pytest
   ```

## Bonus Features

- The ETL solution includes basic error handling, logging, container health checks and unit tests for enhanced robustness and maintainability.

## Dependencies

- Python dependencies are specified in the `requirements.txt` file.
- The required Python packages will be installed during the Docker build process.

## Best Practices for Production

Consider the following best practices that are commonly used in production environments but not necessarily followed in this solution:

Secrets Management: In a production environment, sensitive information such as database credentials should be managed using secure and dedicated solutions like HashiCorp Vault or Docker Secrets.

Monitoring and Logging: Implement a robust monitoring and logging solution to track the performance, errors, and overall health of the ETL process in a production environment.

Container Orchestration: Utilize container orchestration tools like Kubernetes or Docker Swarm for better scalability, deployment management, and resource utilization.

Data Validation and Cleaning: Enhance data validation and cleaning processes to handle edge cases and ensure data accuracy and consistency in production.

Parallel Processing: Implement parallel processing to improve the speed and efficiency of the ETL process, especially when dealing with large datasets.

Automated Testing: Extend test coverage with automated integration tests and end-to-end tests to catch potential issues before deployment.


## Other packages used (not listed in requirements.txt)

- black (formatting)
- flake8 (linting)
- coverage (test coverage)


