# Project Setup Guide
setting up and running the Big Data Pipeline environment using Docker Compose. It includes HDFS, Spark, Hive, and tools for querying and analyzing MIMIC-III data.

## Prerequisites

Make sure you have the following installed:

- [Docker Desktop](https://www.docker.com/products/docker-desktop)
- [Git](https://git-scm.com/)
- (Optional) [DBeaver](https://dbeaver.io/) — for GUI Hive queries via Beeline
- 8 GB RAM or more recommended

Step 2: Start Docker Cluster
bash
Copy
Edit
docker-compose up -d
This starts the following services:

namenode, datanode — for HDFS

spark-master, spark-worker — for PySpark

hive-server, hive-metastore — for Hive queries

Wait until all containers are up (docker ps to check).

📁 Step 3: Copy MIMIC-III Data into HDFS
You can copy your raw CSVs using the provided batch script:

On Windows:
cmd
Copy
Edit
scripts\copy_to_hdfs.bat
When prompted, enter a full path like:

makefile
Copy
Edit
C:\Users\yourname\Desktop\mimic_data\clinical_db
🔄 Step 4: Run PySpark Script to Clean and Convert Data
This reads raw CSVs from HDFS, cleans them, and writes Parquet files.

Option A — via script:
cmd
Copy
Edit
scripts\run_spark.bat
Then enter:

makefile
Copy
Edit
C:\Users\yourname\Desktop\big_data_project\data_pipeline\clean_admissions.py
Option B — manually (inside Spark container):
bash
Copy
Edit
docker exec -it spark-master bash
/spark/bin/spark-submit --master spark://spark-master:7077 /path/to/clean_admissions.py
🗂️ Step 5: Access Hive and Build External Tables
You can use Hive CLI or Beeline.

Option A — CLI inside container:
bash
Copy
Edit
docker exec -it hive-server bash
beeline -u jdbc:hive2://localhost:10000
Option B — Connect via DBeaver:
Driver: Hive

JDBC URL: jdbc:hive2://localhost:10000

User: hive

Password: (leave blank)

Now you can create external tables:

sql
Copy
Edit
CREATE EXTERNAL TABLE admissions (
  subject_id INT,
  hadm_id INT,
  ...
)
STORED AS PARQUET
LOCATION '/clinical_db_transformed/ADMISSIONS';
📊 Step 6: Run Hive Queries
Run queries like:

sql
Copy
Edit
SELECT gender, COUNT(*) FROM patients GROUP BY gender;
SELECT admission_type, AVG(stay_length) FROM admissions GROUP BY admission_type;
🧼 Optional: Stop Everything
To stop the entire cluster:

bash
Copy
Edit
docker-compose down
t






