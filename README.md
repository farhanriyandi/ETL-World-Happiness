# ETL-World-Happiness

## Overview
The World Happiness project was created to implement the ETL (Extract, Transform, Load) process using a data pipeline through web scraping. The extracted data includes national happiness rankings along with the continent of each country, which is then transformed and loaded into PostgreSQL.

## Requirements
To run this project, you need:
1. Docker & Docker Compose: To set up the Airflow and PostgreSQL services.
2. Python 3.7+: For the ETL scripts.
3. Apache Airflow: To manage and schedule the ETL pipeline.
4. Pandas Library: For data transformation.

## Setup Instructions
### 1. Installation & Configuration
Clone the project repository:
```
git clone https://github.com/farhanriyandi/ETL-World-Happiness.git
cd <local-project-directory>
```
### 2. Create a Virtual Environment
Before starting the project, it's recommended to create a virtual environment to manage the dependencies in your local project directory:
```
python -m venv .venv
source .venv/Scripts/activate
```
### 3. Install all dependencies
```
pip install -r requirements.txt
```
### 4. Build and Start the Docker Containers
Use the provided `docker-compose.yml` to set up Airflow and PostgreSQL:
```
docker-compose up -d
```
### 5. Postgres connection configuration

![image](https://github.com/user-attachments/assets/c20be675-aaaa-4cda-aa23-0d3a5309a1d7)


### 6. Airflow connection configuration
* open `localhost:8089` to access Airflow.
```
Username: airflow
Password: airflow
```
* setup connection to postgresql
1. select admin menu > connections in the Airflow UI
2. Click on the 'Create' button to add a new connection.
3. Enter the following details as shown in the image below:

![image](https://github.com/user-attachments/assets/2e5d0af7-75af-4538-a69b-75881467d7bb)

`Note` maybe in your host section it can be different in the picture. you can find out your host by opening the command prompt, then type:
```
docker ps
docker exec -it <your_postgres_container_id> bash
```
then type:
`hostname -I`

Then your host will appear, and fill in the host with your own host.

### 7. Running Dag
Select the `DAGs menu > etl_countries_happiness` and trigger the DAG.

![image](https://github.com/user-attachments/assets/1015132f-29d2-4aec-b069-ad3c0783944f)

### 8. Postgres Table Result
![image](https://github.com/user-attachments/assets/57880e80-c961-41e2-9dc6-38813cce3885)








