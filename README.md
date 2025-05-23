# MIMIC-III Big Data Pipeline

This project builds a scalable batch analytics pipeline using MIMIC-III clinical data with tools like Hadoop, Spark, Hive, and Docker.


## Features

- Batch data loading to HDFS
- PySpark-based data cleansing and transformation
- Conversion from raw CSVs to optimized Parquet format
- Hive queries for ICU readmission, mortality, and length-of-stay
- Hive external tables accessible via Beeline or DBeaver
- Modular and reproducible Dockerized infrastructure

---

## Folder Structure

- `scripts/` - ETL and HDFS copy scripts (Bash, Batch, and PySpark)
- `data_pipeline/` - Core Spark jobs for transforming MIMIC-III tables
- `hive_queries/` - HiveQL scripts for querying clinical insights
- `docs/` - Architecture diagrams, setup guides, usage examples

---

## Tools & Technologies

- **Hadoop HDFS** - Distributed storage for raw and transformed clinical data
- **Apache Spark** - In-memory distributed processing engine for data transformation
- **Apache Hive** - SQL interface for analyzing large-scale structured data
- **Docker / Docker Compose** - Containerized setup for replicable development
- **Beeline / DBeaver** - Hive query interfaces
- **Python / PySpark** - Programming for data preprocessing and analysis

---

## How to Run

1. **Clone the project**

   git clone https://github.com/your-username/mimic-big-data-pipeline.git
   cd mimic-big-data-pipeline

2. **Start the Docker cluster**
   docker-compose up -d
   
3.  **Load raw MIMIC-III data to HDFS**
   scripts\copy_to_hdfs.bat

5. **Run Spark preprocessing jobs**
   scripts\run_spark.bat
   
📌 Enter the path to your PySpark script (e.g., scripts\hdfs_admissions_processor.py).

6. **Create Hive external tables**

Use Beeline or DBeaver connected to Hive on port 10000, then execute CREATE EXTERNAL TABLE statements on the Parquet outputs.

7. **Run Hive analytics queries**

Execute queries from hive_queries/ such as:

SELECT diagnosis, AVG(stay_length) FROM admissions GROUP BY diagnosis;

## Available Hive Queries

avg_stay_by_diagnosis.hql — Average hospital stay by diagnosis

icu_readmission_distribution.hql — Distribution of ICU readmissions

mortality_rate_by_age.hql — Death rate by age group

## Documentation

docs/architecture.md — Data pipeline structure and flow

docs/setup_guide.md — Step-by-step setup and usage

docs/hive_queries_explained.md — What each .hql file does

## Troubleshooting

| Problem                | Solution                                         |
| ---------------------- | ------------------------------------------------ |
| Docker not starting    | Restart Docker Desktop                           |
| File not found in HDFS | Double-check `copy_to_hdfs.bat` and HDFS paths   |
| Hive not responding    | Verify `hive-server` container is up, restart it |
| Spark job fails        | Check logs: `docker logs spark-master`           |

## License
This project is licensed under the MIT License

## Author
Ahmed Hisham
🔗 GitHub: iamahmedhisham



