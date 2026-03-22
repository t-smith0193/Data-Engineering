# aws-airflow-ec2-pipeline

---
The goal of this project is to replicate a production-style data engineering stack using:

- AWS EC2 for compute  
- Docker for containerization  
- Apache Airflow for orchestration  

This project evolved from an [initial aviation data pipeline](https://github.com/t-smith0193/Data-Engineering/tree/main/projects/aviation_departure_analytics) orchestrated with Dagster on a local machine. To create a more realistic, production-like setup, I wanted to create a Dockerized Apache Airflow deployment on AWS EC2, allowing pipelines to run independently of local infrastructure. This project serves as the orchestration foundation for future pipelines that will integrate S3 for storage and Databricks for transformation.

Steps:
1. Enable Hyper-V in BIOS to install/run Docker Desktop  
2. [Install Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html) on Docker via docker-compose.yaml. Verified Dockerized Airflow locally by creating and executing a test DAG, with logs captured successfully through the Airflow UI.
     
     <img width="589" height="440" alt="Screenshot 2026-03-22 005021" src="https://github.com/user-attachments/assets/a6f705a3-8902-40c6-abc7-511cddf67bf1" />

3.
