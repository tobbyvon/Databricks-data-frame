-- Databricks notebook source
-- MAGIC %python
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC spark = SparkSession.builder.appName('create_tables').getOrCreate()
-- MAGIC
-- MAGIC clinicaltrial_2019 = spark.read.csv('/FileStore/tables/clinicaltrial_2019.csv', header=True, inferSchema=True, sep='|')
-- MAGIC clinicaltrial_2019.createOrReplaceTempView('clinicaltrial_2019')
-- MAGIC
-- MAGIC clinicaltrial_2020 = spark.read.csv('/FileStore/tables/clinicaltrial_2020.csv', header=True, inferSchema=True, sep='|')
-- MAGIC clinicaltrial_2020.createOrReplaceTempView('clinicaltrial_2020')
-- MAGIC
-- MAGIC clinicaltrial_2021 = spark.read.csv('/FileStore/tables/clinicaltrial_2021.csv', header=True, inferSchema=True, sep='|')
-- MAGIC clinicaltrial_2021.createOrReplaceTempView('clinicaltrial_2021')
-- MAGIC
-- MAGIC pharma = spark.read.csv("/FileStore/tables/pharma.csv", header=True, inferSchema=True, sep=',')
-- MAGIC pharma.createOrReplaceTempView('pharma')
-- MAGIC
-- MAGIC clinicaltrial_2021.display()

-- COMMAND ----------

--Question 1
select count(*) from clinicaltrial_2021

-- COMMAND ----------


--Question 2
select Type, count(Type)
from clinicaltrial_2021
group by Type
order by count(Type)
desc

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import split,explode, count
-- MAGIC
-- MAGIC # Assuming `df` is the name of my dataframe and `Conditions` is the name of the column containing study conditions
-- MAGIC condition_counts = clinicaltrial_2021.select(explode(split(clinicaltrial_2021.Conditions, ",")).alias("Condition"))
-- MAGIC condition_counts.createOrReplaceTempView ('Countclinicaltrial_2021')

-- COMMAND ----------

--Question 3
select Condition, count(Condition)
from Countclinicaltrial_2021
group by Condition
order by count(Condition)
desc
LIMIT 5

-- COMMAND ----------

--Question 4
SELECT DISTINCT Sponsor, COUNT(*) OVER (PARTITION BY Sponsor) AS Count
FROM clinicaltrial_2021 WHERE sponsor NOT IN (SELECT DISTINCT Parent_Company FROM pharma)
ORDER BY Count
DESC
LIMIT 10


-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import split, regexp_replace, to_date, count
-- MAGIC from pyspark.sql.types import IntegerType
-- MAGIC
-- MAGIC # create Completion view
-- MAGIC complete_df = clinicaltrial_2021.select(
-- MAGIC     'Status', 
-- MAGIC     split('Completion', ' ')[0].alias('Completion_month'), 
-- MAGIC     regexp_replace('Completion', ' ', '').alias('Completion')
-- MAGIC )
-- MAGIC complete_df.createOrReplaceTempView('Completion')

-- COMMAND ----------

CREATE OR REPLACE TABLE default.Completioninfo  AS SELECT Status, Completion_month, to_date(Completion, 'MMMyyyy') AS Completion_date 
    FROM Completion

-- COMMAND ----------

--Question 5
SELECT Completion_month, COUNT(*) AS Count 
    FROM Completioninfo 
    WHERE Completion_date LIKE '2021%' AND Status = 'Completed' 
    GROUP BY Completion_month 
    ORDER BY MIN(Completion_date)
