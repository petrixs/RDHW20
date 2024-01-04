Running Airflow:

1. Be sure that you installed docker, docker-compose
- Docker Compose version v2.15.1
- Docker version 20.10.23, build 7155243
2. Run apache airflow initialization
- docker compose up airflow-init
- The account created has the login ```airflow``` and the password ```airflow```
3. Run docker-compose
- ```docker compose up``` - for interactive usage
- ```docker compose up -d``` - detached mode

The airflow GUI is available at:

http://localhost:8080

Airflow docker-compose documentation:

