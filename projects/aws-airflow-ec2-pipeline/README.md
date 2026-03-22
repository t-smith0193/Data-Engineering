# aws-airflow-ec2-pipeline

---
The goal of this project is to replicate a production-style data engineering stack using:

- AWS EC2 for compute  
- Docker for containerization  
- Apache Airflow for orchestration  
- S3 for storage  
- Databricks for transformation

This project evolved from an initial aviation data pipeline orchestrated with Dagster on a local machine. To create a more realistic, production-like setup, I wanted to create a Dockerized Apache Airflow deployment on AWS EC2, allowing pipelines to run independently of local infrastructure.
