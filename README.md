# Databricks-data-frame
This repository cleans and analyzes a csv file on medical study
#Introduction
Clinical trial datasets with the combination of parent company information within a list of pharmaceutical companies dataset  was analysed and basic implementation was to  made to achieve the solutions to the problems. The results  to the analysis is based on the enquiry for further insights on: the number of studies in Clinical trial 2021 dataset,  types of studies in the dataset along with the frequencies of each type, The top 5 conditions with their frequencies, the top ten most common sponsors that are not pharmaceutical companies, along with the number of clinical trials they have sponsored, and a visualization of the number of completed studies each month in a given year. The solutions to these enquires are implemented in RDD, SQL and Data Frame.
The setup required to be able to complete these implementations include creating a cluster  called Assessment Cluster.

#The notebook for RDD implementation is call “Oluwatobiloba_vaughan_RDD”. The language was select as default (Python) and the cluster is attached to my currently created cluster which gives access this notebook.
 
#Likewise, the notebook for SQL implementation is call “Oluwatobiloba_vaughan_SQl”. The language was select as default (SQL) and the cluster is attached to my currently created cluster which gives access this notebook.



 
Also, the notebook for Data Frame implementation is call “Oluwatobiloba_vaughan_Data Frame”. The language was select as default (Python) and the cluster is attached to my currently created cluster which gives access this notebook.To preprepare the data a zipped dataset  containing the necessary files was imported to databricks.   
 
The zip archive is then extracted, Since the dbutils toolkit doesn't provide an unzip command, the zip file was copied to the driver node (local file system), using a shell command, and then having the extracted contents put back into DBFS.
 

 

 
 To clean the dataset, pyspark.sql  was  import from Spark Session and the data was loaded into a dataframe which is in turn used to create an sql view.
 

#Question 1 
The number of studies in the dataset. 
RDD
 
To get the number of studies in the dataset, the view created during data cleaning is loaded into an RDD named clinicaltrial_2021RDD and a lambda function was used to select the row named “Id” which is then counted in the numbers_of_studies dataframe which the print function displays.
SQL
 
To get the number of studies in the dataset using SQL a select count of all rows in the "clinicaltrial_2021" table was made.

DATA FRAME
To get the number of studies in the dataset using data frame,
 
To get the number of studies in the dataset, the view created during data cleaning is loaded into a data frame named “clinicaldf” and a select function was used to select the row named “Id” which is then counted in the studycount dataframe which the print function displays.

#Question 2
List of all the types of studies in the dataset along with the frequencies of each type. 
RDD
 
To get the List of all the types of studies in the dataset along with the frequencies of each type, the view created during data cleaning is loaded into a RDD named “clinicaltrial_2021RDD”  A new RDD named “typefrequencyRDD” is created which map transformations clinicaltrial_2021RDD where each element is a tuple consisting of a unique value from the "Type" column and the integer value 1. The typefrequencyRDD then reduceByKey transformation to group the tuples by key (i.e., the unique value from the "Type" column) and sum the corresponding values (i.e., the integers 1). The next RDD sorts the resulting RDD in descending order based on the count of each unique value. Finally, the code prints out each unique value and its corresponding count.




SQL

 
Using SQL statement to get the List of all the types of studies in the dataset along with the frequencies of each type using select statement to select the "Type" column and counting the frequency of each unique value from the clinicaltrial_2021 table using the GROUP BY clause. The result is then ordered by the count in descending order.

DATA FRAME
 
To get the List of all the types of studies in the dataset along with the frequencies of each type using data frame The code above imports the necessary functions from the PySpark SQL module, it then groups the DataFrame by the "Type" column and counts the number of occurrences of each unique value. The count column is then renamed to "count" using the alias function and sorts the resulting DataFrame in descending order based on the count column. Finally, the code prints out the resulting DataFrame using the show function.

# Question 3
The top 5 conditions (from Conditions) with their frequencies.
RDD
 
To get the top 5 conditions (from Conditions) with their frequencies line the RDD named “clinicaltrial_2021RDD” reads "clinicaltrial_2021" table into an RDD. Using the lamda function “conditionRDD” filters out any records where the "Conditions" field is null and then uses flatMap to split the comma-separated values into individual conditions. The next “conditionRDD” removes any whitespace from the conditions and maps each one to a tuple with a count of 1 and reduces the tuples by condition, summing the counts for each condition.
Next “conditionRDD”  swaps the position of the count and condition in the tuple and sorts the RDD by count in descending order. It then takes the top 5 most frequently occurring conditions. Finally, the code prints out the top 5 conditions and their corresponding counts.

SQL
 
To split conditions column of the clinicaltrial_2021 table dataset, split function was imported from pyspark.sql.functions and a temporary view was named Countclinicaltrial_2021.
 
 
Using SQL query, the "Condition" column and counts the number of occurrences of each unique condition was selected from the "Countclinicaltrial_2021" table. It then groups the results by condition and orders them in descending order by the count. The query limits the output to the top 5 conditions with the highest counts.

DATA FRAME
 
To get the top 5 conditions (from Conditions) with their frequencies using data frame the split, explode, count is  imported from the PySpark SQL library,  and "Conditions" column is selected from the "clinicaldf" DataFrame and uses the "split" function to split the comma-separated values into individual conditions. The "explode" function is then used to create a new row for each condition, effectively "exploding" the original row into multiple rows and filter function filters out any rows where the condition is null, next grouping the rows by condition and aggregates the count of each group using the "agg" function and the "count" function. The resulting column is aliased as "Count". Finally using the order by function to order the resulting DataFrame by the "Count" column in descending order and takes the top 5 results using the "limit" function.


# Question 4
Find the 10 most common sponsors that are not pharmaceutical companies, along with the number of clinical trials they have sponsored.
RDD
 
To get the 10 most common sponsors that are not pharmaceutical companies, along with the number of clinical trials they have sponsored the PySpark code is processing data from two tables, "pharma" and "clinicaltrial_2021". First the "pharma" table is read into an RDD and maps each row to the "Parent_Company" column. Next, the "clinicaltrial_2021" table is also read into an RDD and maps each row to the "Sponsor" column. Then the filterRDD subtracts the "pharmaRDD" from the "clinicaltrial_2021RDD" to create an RDD of sponsors that are not in the pharma dataset. It then maps each sponsor to a tuple with a count of 1 and reduces the tuples by sponsor, summing the counts for each sponsor. The sponsorRDD swaps the position of the count and sponsor in the tuple and sorts the RDD by count in descending order. It then takes the top 10 sponsors. Finally, the code prints out the top 10 sponsors and their corresponding counts.
SQL
 
To get the 10 most common sponsors that are not pharmaceutical companies, along with the number of clinical trials they have sponsored using the SQL query the "Sponsor" column is selected from the "clinicaltrial_2021" table and uses the "DISTINCT" keyword to eliminate any duplicate sponsors. It then uses the "COUNT(*) OVER (PARTITION BY Sponsor)" syntax to add a column called "Count" that counts the number of occurrences of each sponsor. The query then filters out any sponsors that are in the "pharma" table using a subquery that selects the distinct "Parent_Company" values from the "pharma" table. The results are then ordered by the "Count" column in descending order and limited to the top 10 using the "LIMIT" keyword.

DATA FRAME
 
To get the 10 most common sponsors that are not pharmaceutical companies, along with the number of clinical trials they have sponsored using the data frame implementation,the code first reads in the "pharma" and "clinicaltrial_2021" tables as DataFrames. The next data frame “pharmadf” joins the two tables on the "Parent_Company" and "Sponsor" columns, respectively, using a left anti join to eliminate any rows where there is a match in both tables. Next the group by function groups the resulting DataFrame by the "Sponsor" column and aggregates the "Sponsor" column using the "count" function.The next line renames the resulting column to "Count" using the "withColumnRenamed" function. The fifth line orders the DataFrame by the "Count" column in descending order and limits the output to the top 10 rows using the "limit" function. Finally, the code prints out the resulting DataFrame using the "show" function.

#Question 5
Plot of the Number of completed studies each month in a given year – for the submission dataset, the year is 2021.
RDD
 

 
Number of completed studies each month in a given year Imported split, regexp_replace, to_date, year, functions and libraries from PySpark. clinicaltrial_2021 read the data from the table "clinicaltrial_2021", Next completion extracts the completion month and year from the "Completion" column using PySpark's split and regexp_replace functions and store the results in a new DataFrame named completion, Filtering the completion DataFrame to include only the rows with completion year equal to 2021 and status equal to "Completed", and store the result in a new DataFrame named complete,Create a dictionary that maps month names to their corresponding numbers. Use PySpark's RDD API to transform the complete DataFrame into an RDD of (month, count) pairs, where count is always 1, and then reduce the RDD by month to get the total count of completed trials per month.
Sort the results by month number. Print the results to the console.
 
To plot a bar chart using the result from countRDD, matplotlib.pyplot  was imported as using the Matplotlib library to create a bar chart showing the number of completed studies each month in 2021. The months list contains the names of the months and the counts list contains the number of completed studies for each month. The code uses the plt.bar function to create a vertical bar chart, where each bar represents a month and the height of the bar represents the number of completed studies. The color parameter is used to set the color of the bars to green. The plt.xlabel, plt.ylabel, and plt.title functions are used to add axis labels and a title to the chart. Finally, the plt.show function is used to display the chart.
Overall, this code is a simple and effective way to visualize the number of completed studies each month in 2021 using a bar chart.
