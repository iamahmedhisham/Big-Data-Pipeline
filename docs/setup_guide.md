# Project Setup Guide
setting up and running the Big Data Pipeline environment using Docker Compose. It includes HDFS, Spark, Hive, and tools for querying and analyzing MIMIC-III data.

## Prerequisites

Make sure you have the following installed:

- [Docker Desktop](https://www.docker.com/products/docker-desktop)
- [Git](https://git-scm.com/)
- (Optional) [DBeaver](https://dbeaver.io/) — for GUI Hive queries via Beeline
- 8 GB RAM or more recommended

  ## Step 1: Clone the Repository
  ```bash
git clone https://github.com/iamahmedhisham/big_data_project.git
cd big_data_project
  ## Step 2: Start Docker Cluster
docker-compose up -d

This starts the following services:
namenode, datanode — for HDFS
spark-master, spark-worker — for PySpark
hive-server, hive-metastore — for Hive queries
Wait until all containers are up (docker ps to check).

  ## Step 3: Copy MIMIC-III Data into HDFS
  On Windows: 
  scripts\copy_to_hdfs.bat
  ## Step 4: Run PySpark Script to Clean and Convert Data
  docker exec -it spark-master bash
/spark/bin/spark-submit --master spark://spark-master:7077 /path/to/clean_admissions.py
  ## Step 5: Access Hive and Build External Tables
   Connect via DBeaver:
Driver: Hive
JDBC URL: jdbc:hive2://localhost:10000
  ## Step 6: Run Hive Queries
  Run queries like:
  SELECT gender, COUNT(*) FROM patients GROUP BY gender;
  SELECT admission_type, AVG(stay_length) FROM admissions GROUP BY admission_type;







