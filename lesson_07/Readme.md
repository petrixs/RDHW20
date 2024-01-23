Running Airflow:

1. Check installed docker, docker-compose, my:
- Docker Compose version v2.15.1
- Docker version 20.10.23, build 7155243
2. Download docker-compose.yml from https://airflow.apache.org/docs/apache-airflow/2.8.0/docker-compose.yaml
3. Run apache airflow initialization
- ```docker compose up airflow-init```
- The account created has the login ```airflow``` and the password ```airflow```
4. Run docker-compose
- ```docker compose up```

The airflow GUI is available at:

http://localhost:8080

Airflow docker-compose documentation:

https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

