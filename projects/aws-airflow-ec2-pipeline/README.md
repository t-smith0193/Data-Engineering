# aws-airflow-ec2-pipeline

---
The goal of this project is to replicate a production-style data engineering stack using:

- AWS EC2 for compute  
- Docker for containerization  
- Apache Airflow for orchestration  

This project evolved from an [initial aviation data pipeline](https://github.com/t-smith0193/Data-Engineering/tree/main/projects/aviation_departure_analytics) orchestrated with Dagster on a local machine. To create a more realistic, production-like setup, I wanted to create a Dockerized Apache Airflow deployment on AWS EC2, allowing pipelines to run independently of local infrastructure. Additionally, I implemented system health monitoring using CloudWatch, SNS, Lambda, and Slack to deliver real-time alerts for CPU utilization and EC2 instance status. This project serves as the orchestration foundation for future pipelines that will integrate S3 for storage and Databricks for transformation.

Steps:
1. Enable Hyper-V in BIOS to install/run Docker Desktop  
2. [Install Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html) on Docker via docker-compose.yaml. Verified Dockerized Airflow locally by creating and executing a test DAG, with logs captured successfully through the Airflow UI.
     
3. Launched an EC2 instance and set up the security group to only allow SSH from my IP, keeping everything else closed off. Instead of opening port 8080 to the public, I’m using SSH port forwarding to access the Airflow UI locally.  
4. Installed Docker on EC2 instance.  
     <img width="709" height="60" alt="Screenshot 2026-03-22 014647" src="https://github.com/user-attachments/assets/0322d627-e0a1-4ba4-8c5b-4517c8e3f032" />

5. Successfully deployed Apache Airflow instance on AWS EC2 via Docker and verified DAG execution in a cloud environment.

     <img width="589" height="440" alt="Screenshot 2026-03-22 005021" src="https://github.com/user-attachments/assets/a6f705a3-8902-40c6-abc7-511cddf67bf1" />

6. Set up CloudWatch-based monitoring with SNS, Lambda, and Slack integrations to alert on EC2 CPU utilization and instance health.  

      <img width="271" height="242" alt="image" src="https://github.com/user-attachments/assets/eb9246c0-1a1a-4a3c-b77f-5326a69d3467" />
      <img width="172" height="207" alt="image" src="https://github.com/user-attachments/assets/9d70de58-476d-4995-9c61-90d8ef15579b" />  

7. Enabled additional montioring by installing Amazon CloudWatch Agent onto EC2 instance directly and implemented IAM role-based authentication.  
   
      <img width="911" height="149" alt="image" src="https://github.com/user-attachments/assets/6a689668-2f33-4c63-97ae-a58b31a109b1" />

8. Extended Apache Airflow Docker environment to include custom dependencies (boto3, psutil) and built a system health monitoring DAG (system_health_heartbeat) that collects runtime metrics and publishes hourly alerts via AWS SNS and Slack.

      <img width="352" height="235" alt="image" src="https://github.com/user-attachments/assets/61306281-e94d-416e-8f91-7fbf0a2c48cc" />




