# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %sql
# MAGIC --SCD type 1 table creation
# MAGIC CREATE TABLE IF NOT EXISTS scd1_customer (
# MAGIC     Customer_ID INT,
# MAGIC     Customer_Name STRING,
# MAGIC     City STRING,
# MAGIC     Start_Date DATE,
# MAGIC     End_Date DATE,
# MAGIC     Current_Flag STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/FileStore/tables/scd/scd1_customer'
# MAGIC TBLPROPERTIES ('delta.autoOptimize.autoCompact' = 'true', 'delta.autoOptimize.optimizeWrite' = 'true');
# MAGIC
# MAGIC --SCD type 2 table creation
# MAGIC CREATE TABLE IF NOT EXISTS scd2_customer (
# MAGIC     Customer_ID INT,
# MAGIC     Customer_Name STRING,
# MAGIC     City STRING,
# MAGIC     Start_Date DATE,
# MAGIC     End_Date DATE,
# MAGIC     Current_Flag STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/FileStore/tables/scd/scd2_customer'
# MAGIC TBLPROPERTIES ('delta.autoOptimize.autoCompact' = 'true', 'delta.autoOptimize.optimizeWrite' = 'true');
# MAGIC
# MAGIC --SCD type 3 table creation
# MAGIC CREATE TABLE IF NOT EXISTS scd3_customer (
# MAGIC     Customer_ID INT,
# MAGIC     Customer_Name STRING,
# MAGIC     City STRING,
# MAGIC     Previous_City STRING,
# MAGIC     Start_Date DATE
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/FileStore/tables/scd/scd3_customer'
# MAGIC TBLPROPERTIES ('delta.autoOptimize.autoCompact' = 'true', 'delta.autoOptimize.optimizeWrite' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC --SCD type 1 table data
# MAGIC INSERT INTO scd1_customer (Customer_ID, Customer_Name, City, Start_Date, End_Date, Current_Flag) VALUES
# MAGIC (1, 'John Doe', 'New York', '2020-01-01', NULL, 'Y'),
# MAGIC (2, 'Jane Smith', 'Boston', '2020-03-15', NULL, 'Y'),
# MAGIC (3, 'Mark Johnson', 'Chicago', '2020-06-01', NULL, 'Y'),
# MAGIC (4, 'Sarah Davis', 'Dallas', '2020-08-05', NULL, 'Y'),
# MAGIC (5, 'Amy Clark', 'Los Angeles', '2020-10-10', NULL, 'Y'),
# MAGIC (6, 'David Wilson', 'Seattle', '2021-01-20', NULL, 'Y'),
# MAGIC (7, 'Olivia Brown', 'Miami', '2021-02-18', NULL, 'Y'),
# MAGIC (8, 'Liam Miller', 'San Francisco', '2021-03-22', NULL, 'Y'),
# MAGIC (9, 'Emma Moore', 'Austin', '2021-04-10', NULL, 'Y'),
# MAGIC (10, 'James Taylor', 'San Diego', '2021-05-06', NULL, 'Y'),
# MAGIC (11, 'Sophia Anderson', 'Houston', '2021-06-11', NULL, 'Y'),
# MAGIC (12, 'Jackson Thomas', 'Philadelphia', '2021-07-13', NULL, 'Y'),
# MAGIC (13, 'Mason Martinez', 'Chicago', '2021-08-17', NULL, 'Y'),
# MAGIC (14, 'Amelia Harris', 'Dallas', '2021-09-14', NULL, 'Y'),
# MAGIC (15, 'Benjamin Clark', 'Los Angeles', '2021-10-12', NULL, 'Y'),
# MAGIC (16, 'Ella Rodriguez', 'Boston', '2021-11-15', NULL, 'Y'),
# MAGIC (17, 'Lucas Perez', 'New York', '2021-12-20', NULL, 'Y'),
# MAGIC (18, 'Harper Wilson', 'Seattle', '2022-01-19', NULL, 'Y'),
# MAGIC (19, 'Henry Lewis', 'Miami', '2022-02-10', NULL, 'Y'),
# MAGIC (20, 'Ethan Walker', 'San Francisco', '2022-03-25', NULL, 'Y'),
# MAGIC (21, 'Madison Young', 'Austin', '2022-04-30', NULL, 'Y'),
# MAGIC (22, 'Aiden King', 'San Diego', '2022-05-11', NULL, 'Y'),
# MAGIC (23, 'Isabella Scott', 'Houston', '2022-06-15', NULL, 'Y'),
# MAGIC (24, 'Matthew Green', 'Philadelphia', '2022-07-12', NULL, 'Y'),
# MAGIC (25, 'Jack Adams', 'Chicago', '2022-08-03', NULL, 'Y'),
# MAGIC (26, 'Lily Nelson', 'Dallas', '2022-09-08', NULL, 'Y'),
# MAGIC (27, 'Alexander Carter', 'Los Angeles', '2022-10-05', NULL, 'Y'),
# MAGIC (28, 'Charlotte Mitchell', 'Boston', '2022-11-09', NULL, 'Y'),
# MAGIC (29, 'Samuel Perez', 'New York', '2022-12-01', NULL, 'Y'),
# MAGIC (30, 'Grace Roberts', 'Seattle', '2023-01-10', NULL, 'Y');
# MAGIC
# MAGIC --SCD type 2 table data
# MAGIC INSERT INTO scd2_customer (Customer_ID, Customer_Name, City, Start_Date, End_Date, Current_Flag) VALUES
# MAGIC (1, 'John Doe', 'New York', '2020-01-01', NULL, 'Y'),
# MAGIC (2, 'Jane Smith', 'Boston', '2020-03-15', NULL, 'Y'),
# MAGIC (3, 'Mark Johnson', 'Chicago', '2020-06-01', NULL, 'Y'),
# MAGIC (4, 'Sarah Davis', 'Dallas', '2020-08-05', NULL, 'Y'),
# MAGIC (5, 'Amy Clark', 'Los Angeles', '2020-10-10', NULL, 'Y'),
# MAGIC (6, 'David Wilson', 'Seattle', '2021-01-20', NULL, 'Y'),
# MAGIC (7, 'Olivia Brown', 'Miami', '2021-02-18', NULL, 'Y'),
# MAGIC (8, 'Liam Miller', 'San Francisco', '2021-03-22', NULL, 'Y'),
# MAGIC (9, 'Emma Moore', 'Austin', '2021-04-10', NULL, 'Y'),
# MAGIC (10, 'James Taylor', 'San Diego', '2021-05-06', NULL, 'Y'),
# MAGIC (11, 'Sophia Anderson', 'Houston', '2021-06-11', NULL, 'Y'),
# MAGIC (12, 'Jackson Thomas', 'Philadelphia', '2021-07-13', NULL, 'Y'),
# MAGIC (13, 'Mason Martinez', 'Chicago', '2021-08-17', NULL, 'Y'),
# MAGIC (14, 'Amelia Harris', 'Dallas', '2021-09-14', NULL, 'Y'),
# MAGIC (15, 'Benjamin Clark', 'Los Angeles', '2021-10-12', NULL, 'Y'),
# MAGIC (16, 'Ella Rodriguez', 'Boston', '2021-11-15', NULL, 'Y'),
# MAGIC (17, 'Lucas Perez', 'New York', '2021-12-20', NULL, 'Y'),
# MAGIC (18, 'Harper Wilson', 'Seattle', '2022-01-19', NULL, 'Y'),
# MAGIC (19, 'Henry Lewis', 'Miami', '2022-02-10', NULL, 'Y'),
# MAGIC (20, 'Ethan Walker', 'San Francisco', '2022-03-25', NULL, 'Y'),
# MAGIC (21, 'Madison Young', 'Austin', '2022-04-30', NULL, 'Y'),
# MAGIC (22, 'Aiden King', 'San Diego', '2022-05-11', NULL, 'Y'),
# MAGIC (23, 'Isabella Scott', 'Houston', '2022-06-15', NULL, 'Y'),
# MAGIC (24, 'Matthew Green', 'Philadelphia', '2022-07-12', NULL, 'Y'),
# MAGIC (25, 'Jack Adams', 'Chicago', '2022-08-03', NULL, 'Y'),
# MAGIC (26, 'Lily Nelson', 'Dallas', '2022-09-08', NULL, 'Y'),
# MAGIC (27, 'Alexander Carter', 'Los Angeles', '2022-10-05', NULL, 'Y'),
# MAGIC (28, 'Charlotte Mitchell', 'Boston', '2022-11-09', NULL, 'Y'),
# MAGIC (29, 'Samuel Perez', 'New York', '2022-12-01', NULL, 'Y'),
# MAGIC (30, 'Grace Roberts', 'Seattle', '2023-01-10', NULL, 'Y');
# MAGIC
# MAGIC --SCD type 3 table data
# MAGIC INSERT INTO scd3_customer (Customer_ID, Customer_Name, City,Previous_City, Start_Date) VALUES
# MAGIC (1, 'John Doe', 'New York',NULL, '2020-01-01'),
# MAGIC (2, 'Jane Smith', 'Boston',NULL, '2020-03-15'),
# MAGIC (3, 'Mark Johnson', 'Chicago',NULL, '2020-06-01'),
# MAGIC (4, 'Sarah Davis', 'Dallas',NULL, '2020-08-05'),
# MAGIC (5, 'Amy Clark', 'Los Angeles',NULL, '2020-10-10'),
# MAGIC (6, 'David Wilson', 'Seattle',NULL, '2021-01-20'),
# MAGIC (7, 'Olivia Brown', 'Miami',NULL, '2021-02-18'),
# MAGIC (8, 'Liam Miller', 'San Francisco',NULL, '2021-03-22'),
# MAGIC (9, 'Emma Moore', 'Austin',NULL, '2021-04-10'),
# MAGIC (10, 'James Taylor', 'San Diego',NULL, '2021-05-06'),
# MAGIC (11, 'Sophia Anderson', 'Houston',NULL, '2021-06-11'),
# MAGIC (12, 'Jackson Thomas', 'Philadelphia',NULL, '2021-07-13'),
# MAGIC (13, 'Mason Martinez', 'Chicago',NULL, '2021-08-17'),
# MAGIC (14, 'Amelia Harris', 'Dallas',NULL, '2021-09-14'),
# MAGIC (15, 'Benjamin Clark', 'Los Angeles',NULL, '2021-10-12'),
# MAGIC (16, 'Ella Rodriguez', 'Boston',NULL, '2021-11-15'),
# MAGIC (17, 'Lucas Perez', 'New York',NULL, '2021-12-20'),
# MAGIC (18, 'Harper Wilson', 'Seattle',NULL, '2022-01-19'),
# MAGIC (19, 'Henry Lewis', 'Miami',NULL, '2022-02-10'),
# MAGIC (20, 'Ethan Walker', 'San Francisco',NULL, '2022-03-25'),
# MAGIC (21, 'Madison Young', 'Austin',NULL, '2022-04-30'),
# MAGIC (22, 'Aiden King', 'San Diego',NULL, '2022-05-11'),
# MAGIC (23, 'Isabella Scott', 'Houston',NULL, '2022-06-15'),
# MAGIC (24, 'Matthew Green', 'Philadelphia',NULL, '2022-07-12'),
# MAGIC (25, 'Jack Adams', 'Chicago',NULL, '2022-08-03'),
# MAGIC (26, 'Lily Nelson', 'Dallas',NULL, '2022-09-08'),
# MAGIC (27, 'Alexander Carter', 'Los Angeles',NULL, '2022-10-05'),
# MAGIC (28, 'Charlotte Mitchell', 'Boston',NULL, '2022-11-09'),
# MAGIC (29, 'Samuel Perez', 'New York',NULL, '2022-12-01'),
# MAGIC (30, 'Grace Roberts', 'Seattle',NULL, '2023-01-10');
# MAGIC

# COMMAND ----------

#Sample df creation for analaysis of SCD

data = [
    (1, "John Doe", "Boston"),
    (2, "Jane Smith", "Los Angeles"),
    (3, "Mike Johnson", "San Francisco")
]

# Define the schema using StructType
schema = StructType([
    StructField("Customer_ID", IntegerType(), False),
    StructField("Customer_Name", StringType(), False),
    StructField("City", StringType(), False)
])

# Create DataFrame using the schema
df = spark.createDataFrame(data, schema)
df=df.withColumn("Start_Date",lit(current_date()))

# Register DataFrame as a temporary view
df.createOrReplaceTempView("staging_table")



# COMMAND ----------

# MAGIC %sql 
# MAGIC --Output of the staging table where we can see the records are going to be be update based on SCD
# MAGIC select * from staging_table

# COMMAND ----------

# MAGIC %md
# MAGIC #SCD TYPE 1 Explanation

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- SCD_type 1 Data before going to update based on staging tbales records
# MAGIC
# MAGIC select * from  scd1_customer
# MAGIC where Customer_ID in(1,2,3)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- In SCD Type 1, if there is a change in Customer_Name or City, the existing data is overwritten without preserving historical information
# MAGIC
# MAGIC MERGE INTO scd1_customer AS target
# MAGIC USING staging_table AS source
# MAGIC ON target.Customer_ID = source.Customer_ID
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET
# MAGIC         target.Customer_Name = source.Customer_Name,
# MAGIC         target.City = source.City,
# MAGIC         target.Start_Date = current_date()
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (Customer_ID, Customer_Name, City, Start_Date, End_Date, Current_Flag)
# MAGIC     VALUES (source.Customer_ID, source.Customer_Name, source.City, source.Start_Date, NULL, 'Y');
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- SCD_type 1 Data after updating based on staging tbales records
# MAGIC
# MAGIC select * from  scd1_customer
# MAGIC where Customer_ID in(1,2,3)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Explanation:
# MAGIC - **Customer 1**'s City was changed from "New York" to "Boston" and the change is reflected in the same row. The previous values are not retained.
# MAGIC - **Customer 2**'s  City was changed from "Boston" to "Los Angeles" and the change is reflected in the same row. The previous values are not retained.
# MAGIC - **Customer 3**'s City was changed from "Chicago" to "San Francisco" and the change is reflected in the same row.
# MAGIC
# MAGIC ---
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #SCD TYPE 2 Explanation

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- SCD_type 2 Data before going to update based on staging tbales records
# MAGIC
# MAGIC select * from  scd2_customer
# MAGIC where Customer_ID in(1,2,3)

# COMMAND ----------

# MAGIC %md
# MAGIC In SCD Type 2, a new row is added for each change, while maintaining the historical record. The End_Date is updated for the old row, and the Current_Flag is set to 'N' for the expired record, while the new record gets a Current_Flag of 'Y'.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- it will update the existing records to 'D' flag to identified as deleted or Old records and insert the records which doesnot match
# MAGIC MERGE INTO scd2_customer AS target
# MAGIC USING staging_table AS source
# MAGIC ON target.Customer_ID = source.Customer_ID AND target.Current_Flag = 'Y'
# MAGIC WHEN MATCHED AND (
# MAGIC     target.Customer_Name != source.Customer_Name OR
# MAGIC     target.City != source.City
# MAGIC ) THEN
# MAGIC     UPDATE SET
# MAGIC         target.End_Date = CURRENT_DATE(),
# MAGIC         target.Current_Flag = 'D'      
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (Customer_ID, Customer_Name, City, Start_Date, End_Date, Current_Flag)
# MAGIC     VALUES (source.Customer_ID, source.Customer_Name, source.City, CURRENT_DATE(), NULL, 'Y');
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC INSERT into scd2_customer (Customer_ID, Customer_Name, City, Start_Date, End_Date, Current_Flag)
# MAGIC Select source.Customer_ID, source.Customer_Name, source.City, CURRENT_DATE(), Null, "Y"
# MAGIC FROM staging_table AS source
# MAGIC LEFT JOIN scd2_customer AS target
# MAGIC     ON target.Customer_ID = source.Customer_ID
# MAGIC     AND target.Current_Flag = 'D'
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- SCD_type 2 Data after updating based on staging tbales records
# MAGIC
# MAGIC select * from  scd2_customer
# MAGIC where Customer_ID in(1,2,3)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Explanation of Behavior Based on the Insert Command:**
# MAGIC
# MAGIC - **Change of `Current_Flag` from `"Y"` or `"D"`:**
# MAGIC     - When executing the insert command, Based on the implement of the this insert command the (target.Current_Flag = "Y") will change from `"D"` or `"Y"`.
# MAGIC     so if we implement thge insert command before we ahve to go with `"Y"` for viceversa `"D"`
# MAGIC
# MAGIC - **Observation:**
# MAGIC     - In this command, we are checking the `Current_Flag` values to determine whether the data should be updated or inserted.
# MAGIC     - If the `Current_Flag = 'Y'` or `Current_Flag = 'D'`, the data will be ingested because we're not directly using the target table during the join.
# MAGIC     - **Key Point:** When we use the target table (`scd2_customer`), if the join condition is partially satisfied, it may result in `NULL` values for partially satiscified columns values. 
# MAGIC - **Takeaway:**
# MAGIC
# MAGIC     - We will requir both merge and insert command to handle the SCD type 2
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #SCD TYPE 3 Explanation

# COMMAND ----------

# MAGIC %sql
# MAGIC --is used to store only a limited history of changes. It typically tracks the current and one previous version of the data. This is often used when only tracking a small number of changes (e.g., the most recent update) is needed.
# MAGIC MERGE INTO scd3_customer AS target
# MAGIC USING staging_table AS source
# MAGIC ON target.Customer_ID = source.Customer_ID
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET 
# MAGIC         target.Previous_City = target.City, 
# MAGIC         target.City = source.City, 
# MAGIC         target.Start_Date = CURRENT_DATE()
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (Customer_ID, Customer_Name, City,Previous_City, Start_Date)
# MAGIC     VALUES (source.Customer_ID, source.Customer_Name, source.City, NULL,CURRENT_DATE());
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from scd3_customer
# MAGIC where Customer_ID in(1,2,3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## How SCD Type 3 Works:
# MAGIC
# MAGIC In SCD Type 3, you have two columns to store historical data:
# MAGIC
# MAGIC 1. **Current Value Column**: This column stores the current value of the attribute.
# MAGIC 2. **Previous Value Column**: This column stores the previous value of the attribute.
# MAGIC
# MAGIC When there is a change in the attribute:
# MAGIC - The **Previous Value Column** is updated with the old value.
# MAGIC - The **Current Value Column** is updated with the new value.
# MAGIC
# MAGIC This method allows you to track the change for a limited period (the previous and current values), but unlike Type 2, it does not store a complete history of all changes over time.
# MAGIC
# MAGIC - In our case we ahve only implemented to City column so we can see the changes 
# MAGIC
# MAGIC ---
