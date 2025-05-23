from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, to_timestamp, year, when, isnan, count, datediff, lit,
    lower, upper, regexp_replace, trim, current_date
)

def main():
    # Create one Spark session for all transformations
    spark = SparkSession.builder.appName("MIMIC-Cleaning-Transformation").getOrCreate()

    # -----------------#
    # ADMISSIONS       #
    # -----------------#
    print(">>> Transforming ADMISSIONS...")
    df_adm = spark.read.csv(
        "hdfs://namenode:9000/clinical_db/ADMISSIONS.csv",
        header=True, inferSchema=True
    )

    # Drop rows with all columns null & ensure essential columns are non-null
    df_adm = df_adm.dropna(how="all")
    df_adm = df_adm.dropna(subset=["subject_id", "hadm_id", "admittime", "dischtime"])

    # Convert datetime columns to date
    df_adm = df_adm.withColumn("admittime", to_date("admittime")) \
                   .withColumn("dischtime", to_date("dischtime")) \
                   .withColumn("deathtime", to_date("deathtime"))

    # Filter out invalid discharge times
    df_adm = df_adm.filter(col("dischtime") >= col("admittime"))

    # Calculate length of stay (in days) & filter out extreme values
    df_adm = df_adm.withColumn("stay_length", datediff("dischtime", "admittime"))
    df_adm = df_adm.filter((col("stay_length") > 0) & (col("stay_length") < 365))

    # Cleanup diagnosis text
    df_adm = df_adm.withColumn("diagnosis", lower(regexp_replace("diagnosis", r"[^a-zA-Z0-9\s]", "")))

    # Show admission type distribution & group rare categories into 'OTHER'
    df_adm.groupBy("admission_type").count().show()
    df_adm = df_adm.withColumn(
        "admission_type",
        when(col("admission_type").isin("EMERGENCY", "URGENT", "ELECTIVE"), col("admission_type"))
        .otherwise("OTHER")
    )

    # Write out ADMISSIONS as Parquet
    df_adm.write.mode("overwrite").parquet(
        "hdfs://namenode:9000/clinical_db_transformed/ADMISSIONS"
    )

    # -----------------#
    # PATIENTS         #
    # -----------------#
    print(">>> Transforming PATIENTS...")
    df_pat = spark.read.csv(
        "hdfs://namenode:9000/clinical_db/PATIENTS.csv",
        header=True, inferSchema=True
    )

    df_pat = df_pat.dropna(how='all')
    df_pat = df_pat.dropna(subset=["subject_id", "gender", "dob"])

    # Convert date columns
    df_pat = df_pat.withColumn("dob", to_date("dob")) \
                   .withColumn("dod", to_date("dod"))

    # Filter DOB to a reasonable range
    df_pat = df_pat.filter((year("dob") >= 1900) & (year("dob") <= 2020))

    # Ensure DOD >= DOB or null
    df_pat = df_pat.filter((col("dod").isNull()) | (col("dod") >= col("dob")))

    # Normalize gender
    df_pat = df_pat.withColumn("gender", upper(trim(col("gender"))))
    df_pat = df_pat.withColumn(
        "gender",
        when(col("gender").isin("M", "F"), col("gender"))
        .otherwise("UNKNOWN")
    )

    # Compute approximate age
    df_pat = df_pat.withColumn("age", datediff(current_date(), "dob") / 365)
    # Filter out unrealistic ages
    df_pat = df_pat.filter((col("age") >= 0) & (col("age") < 120))

    # Check nulls
    df_pat.select([count(when(col(c).isNull(), c)).alias(c) for c in df_pat.columns]).show()

    # Write out PATIENTS as Parquet
    df_pat.write.mode("overwrite").parquet(
        "hdfs://namenode:9000/clinical_db_transformed/PATIENTS"
    )

    # -----------------#
    # DIAGNOSES_ICD    #
    # -----------------#
    print(">>> Transforming DIAGNOSES_ICD...")
    df_diag = spark.read.csv(
        "hdfs://namenode:9000/clinical_db/DIAGNOSES_ICD.csv",
        header=True, inferSchema=True
    )

    df_diag = df_diag.dropna(how="all")
    df_diag = df_diag.dropna(subset=["subject_id", "hadm_id", "icd9_code"])

    # Normalize/clean ICD code
    df_diag = df_diag.withColumn("icd_code", upper(trim(col("icd9_code"))))
    df_diag = df_diag.withColumn("icd_code", regexp_replace("icd9_code", r"\.", ""))  # remove dots

    # Example version filter (if your ICD9/ICD10 version is numeric)
    df_diag = df_diag.filter((col("icd9_code") == 9) | (col("icd9_code") == 10))

    # Drop duplicates on key columns
    df_diag = df_diag.dropDuplicates(["subject_id", "hadm_id", "seq_num"])

    # Write out DIAGNOSES_ICD as Parquet
    df_diag.write.mode("overwrite").parquet(
        "hdfs://namenode:9000/clinical_db_transformed/DIAGNOSES_ICD"
    )

    # -----------------#
    # LABEVENTS        #
    # -----------------#
    print(">>> Transforming LABEVENTS...")
    df_lab = spark.read.csv(
        "hdfs://namenode:9000/clinical_db/LABEVENTS.csv",
        header=True, inferSchema=True
    )

    df_lab = df_lab.dropna(how='all')
    df_lab = df_lab.dropna(subset=["subject_id", "hadm_id", "itemid", "charttime", "valuenum"])

    df_lab = df_lab.withColumn("charttime", to_timestamp("charttime"))
    # Filter out invalid numeric lab values if needed
    df_lab = df_lab.filter((col("valuenum") >= 0) & (col("valuenum") <= 10000))

    # Clean up units, value, and flags
    df_lab = df_lab.withColumn("valueuom", upper(trim(col("valueuom"))))
    df_lab = df_lab.withColumn("value", regexp_replace(trim(col("value")), r"\s+", " "))
    df_lab = df_lab.withColumn("flag", upper(trim(col("flag"))))

    # Read D_LABITEMS to join additional label info
    df_lab_items = spark.read.csv(
        "hdfs://namenode:9000/clinical_db/D_LABITEMS.csv",
        header=True, inferSchema=True
    )
    # Join label from D_LABITEMS
    df_lab = df_lab.join(df_lab_items.select("itemid", "label"), on="itemid", how="left")

    # Write out LABEVENTS as Parquet
    df_lab.write.mode("overwrite").parquet(
        "hdfs://namenode:9000/clinical_db_transformed/LABEVENTS"
    )

    # -----------------#
    # ICUSTAYS         #
    # -----------------#
    print(">>> Transforming ICUSTAYS...")
    df_icu = spark.read.csv(
        "hdfs://namenode:9000/clinical_db/ICUSTAYS.csv",
        header=True, inferSchema=True
    )

    df_icu = df_icu.dropna(how="all")
    df_icu = df_icu.dropna(subset=["subject_id", "hadm_id", "icustay_id", "intime", "outtime"])

    # Convert times to timestamp
    df_icu = df_icu.withColumn("intime", to_timestamp("intime")) \
                   .withColumn("outtime", to_timestamp("outtime"))

    # Calculate ICU Stay duration & filter unrealistic
    df_icu = df_icu.withColumn("icu_stay_duration", datediff("outtime", "intime"))
    df_icu = df_icu.filter((col("icu_stay_duration") > 0) & (col("icu_stay_duration") <= 90))

    # Remove duplicates on ICUSTAY_ID
    df_icu = df_icu.dropDuplicates(["icustay_id"])

    # Normalize first_careunit
    df_icu = df_icu.withColumn("first_careunit", upper(trim(col("first_careunit"))))

    # Drop the duration column if no longer needed
    df_icu = df_icu.drop("icu_stay_duration")

    # Write out ICUSTAYS as Parquet
    df_icu.write.mode("overwrite").parquet(
        "hdfs://namenode:9000/clinical_db_transformed/ICUSTAYS"
    )

    # Stop the Spark session
    spark.stop()
    print(">>> All transformations complete.")

if __name__ == "__main__":
    main()
