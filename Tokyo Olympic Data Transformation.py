# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "client_id",
"fs.azure.account.oauth2.client.secret": 'secret_key
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/tanent_id/oauth2/token"}


dbutils.fs.mount(
source = "abfss://tokyo-olympic-data@tokyoolympicmayukh.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/tokyoolymic",
extra_configs = configs)

# COMMAND ----------


configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": "fb0b76ae-1621-4577-b35a-9a2dc9b41d06",
    "fs.azure.account.oauth2.client.secret": 'F9h8Q~xX670q~FUu_5HgrWNUiAXiWstCGdd2CcRk',
    "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/91682f1c-5cb7-4833-86c5-0addc39002bf/oauth2/token"
}

dbutils.fs.mount(
    source = "abfss://tokyo-olympic-data@tokyoolympicmayukh.dfs.core.windows.net",
    mount_point = "/mnt/tokyoolymic",
    extra_configs = configs
)

# COMMAND ----------


# Unmount the existing mount point if it exists
dbutils.fs.unmount("/mnt/tokyoolymic")

# Define the configurations
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": "fb0b76ae-1621-4577-b35a-9a2dc9b41d06",
    "fs.azure.account.oauth2.client.secret": 'F9h8Q~xX670q~FUu_5HgrWNUiAXiWstCGdd2CcRk',
    "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/91682f1c-5cb7-4833-86c5-0addc39002bf/oauth2/token"
}

# Mount the storage
dbutils.fs.mount(
    source = "abfss://tokyo-olympic-data@tokyoolympicmayukh.dfs.core.windows.net",
    mount_point = "/mnt/tokyoolympic",
    extra_configs = configs
)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/tokyoolympic"

# COMMAND ----------

spark

# COMMAND ----------

athletes = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolympic/raw-data/athletes.csv")
coaches = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolympic/raw-data/coaches.csv")
EntriesGender = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolympic/raw-data/EntriesGender.csv")
medals = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolympic/raw-data/medals.csv")
teams = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolympic/raw-data/teams.csv")

# COMMAND ----------

display(athletes)

# COMMAND ----------



# COMMAND ----------

display(coaches)

# COMMAND ----------

display(coaches)

# COMMAND ----------

display(EntriesGender)



# COMMAND ----------

display(medals)

# COMMAND ----------

display(teams)

# COMMAND ----------

EntriesGender.printSchema()

# COMMAND ----------

EntriesGender.schema

# COMMAND ----------

type(EntriesGender)


# COMMAND ----------

EntriesGender.cache()
EntriesGender.printSchema()

# COMMAND ----------

EntriesGender.schema

# COMMAND ----------

EntriesGender.dtypes


# COMMAND ----------

medals.dtypes

# COMMAND ----------

medals.printSchema()

# COMMAND ----------

medals.dtypes

# COMMAND ----------

# Find the top countries with the highest number of gold medals
top_gold_medal_countries = medals.orderBy("Gold", ascending=False).select("Team_Country","Gold").show()

# COMMAND ----------

display(medals)

# COMMAND ----------

# Find the top countries with the highest number of gold medals
top_gold_medal_countries = medals.orderBy("Gold", ascending=False).select("Team/NOC", "Gold")
display(top_gold_medal_countries)

# COMMAND ----------

athletes.repartition(1).write.option("header", "true").mode("overwrite").csv("/mnt/tokyoolympic/transformed-data/athletes")

# COMMAND ----------

coaches.repartition(1).write.option("header", "true").mode("overwrite").csv("/mnt/tokyoolympic/transformed-data/coaches")
EntriesGender.repartition(1).write.option("header", "true").mode("overwrite").csv("/mnt/tokyoolympic/transformed-data/EntriesGender")
medals.repartition(1).write.option("header", "true").mode("overwrite").csv("/mnt/tokyoolympic/transformed-data/medals")
teams.repartition(1).write.option("header", "true").mode("overwrite").csv("/mnt/tokyoolympic/transformed-data/teams")
     

# COMMAND ----------

top_disciplines = (
    medals.groupBy("Discipline")
    .sum("Gold", "Silver", "Bronze")
    .withColumnRenamed("sum(Gold)", "Total_Gold")
    .withColumnRenamed("sum(Silver)", "Total_Silver")
    .withColumnRenamed("sum(Bronze)", "Total_Bronze")
    .withColumn("Total_Medals", col("Total_Gold") + col("Total_Silver") + col("Total_Bronze"))
    .orderBy(col("Total_Medals").desc())
)

top_disciplines.show(10)



# COMMAND ----------

from pyspark.sql.functions import col

# Verify the columns in the DataFrame
print(medals.columns)

# Assuming 'Discipline' is a valid column
top_disciplines = (
    medals.groupBy("Discipline")
    .sum("Gold", "Silver", "Bronze")
    .withColumnRenamed("sum(Gold)", "Total_Gold")
    .withColumnRenamed("sum(Silver)", "Total_Silver")
    .withColumnRenamed("sum(Bronze)", "Total_Bronze")
    .withColumn("Total_Medals", col("Total_Gold") + col("Total_Silver") + col("Total_Bronze"))
    .orderBy(col("Total_Medals").desc())
)

display(top_disciplines.limit(10))

# COMMAND ----------

from pyspark.sql.functions import col

# Group by 'Team/NOC' instead of 'Discipline'
top_disciplines = (
    medals.groupBy("Team/NOC")
    .sum("Gold", "Silver", "Bronze")
    .withColumnRenamed("sum(Gold)", "Total_Gold")
    .withColumnRenamed("sum(Silver)", "Total_Silver")
    .withColumnRenamed("sum(Bronze)", "Total_Bronze")
    .withColumn("Total_Medals", col("Total_Gold") + col("Total_Silver") + col("Total_Bronze"))
    .orderBy(col("Total_Medals").desc())
)

display(top_disciplines.limit(10))

# COMMAND ----------

from pyspark.sql.functions import countDitotal_countries = medals.select(countDistinct("Team_NOC").alias("Total_Countries"))tries"))
total_countries.show()

# COMMAND ----------

from pyspark.sql.functions import col

# Group by country, sum the gold medals, and sort in descending order
top_gold_countries = (
    medals.groupBy("Team/NOC")
    .sum("Gold")
    .withColumnRenamed("sum(Gold)", "Total_Gold")
    .orderBy(col("Total_Gold").desc())
)

# Show top 10 results
top_gold_countries.show(10)


# COMMAND ----------

# Group by Discipline, sum all medals, and sort in descending order
top_disciplines = (
    medals.groupBy("Discipline")
    .sum("Gold", "Silver", "Bronze")
    .withColumnRenamed("sum(Gold)", "Total_Gold")
    .withColumnRenamed("sum(Silver)", "Total_Silver")
    .withColumnRenamed("sum(Bronze)", "Total_Bronze")
    .withColumn("Total_Medals", col("Total_Gold") + col("Total_Silver") + col("Total_Bronze"))
    .orderBy(col("Total_Medals").desc())
)

# Show top 10 sports
top_disciplines.show(10)


# COMMAND ----------

# Group by Team/NOC, sum all medals, and sort in descending order
top_disciplines = (
    medals.groupBy("Team/NOC")
    .sum("Gold", "Silver", "Bronze")
    .withColumnRenamed("sum(Gold)", "Total_Gold")
    .withColumnRenamed("sum(Silver)", "Total_Silver")
    .withColumnRenamed("sum(Bronze)", "Total_Bronze")
    .withColumn("Total_Medals", col("Total_Gold") + col("Total_Silver") + col("Total_Bronze"))
    .orderBy(col("Total_Medals").desc())
)

# Show top 10 sports
top_disciplines.show(10)

# COMMAND ----------



# Group by Discipline and sum male and female participants
gender_comparison = (
    EntriesGender.groupBy("Discipline")
    .sum("Female", "Male")
    .withColumnRenamed("sum(Female)", "Total_Female")
    .withColumnRenamed("sum(Male)", "Total_Male")
    .orderBy(col("Total_Female").desc())  # Sorting by highest female participants
)

# Show top 10 sports
gender_comparison.show(10)

# COMMAND ----------

# Count athletes per country
top_countries_athletes = (
    athletes.groupBy("NOC")
    .count()
    .withColumnRenamed("count", "Total_Athletes")
    .orderBy(col("Total_Athletes").desc())
)

# Show top 10 countries
top_countries_athletes.show(10)


# COMMAND ----------

EntriesGender.show(5)


# COMMAND ----------

# Count athletes per country
top_countries_athletes = (
    athletes.groupBy("NOC")
    .count()
    .withColumnRenamed("count", "Total_Athletes")
    .orderBy(col("Total_Athletes").desc())
)

# Show top 10 countries
top_countries_athletes.show(10)


# COMMAND ----------

# Group by country, sum all medals, and sort in descending order
top_countries_medals = (
    medals.groupBy("Team/NOC")
    .sum("Gold", "Silver", "Bronze")
    .withColumnRenamed("sum(Gold)", "Total_Gold")
    .withColumnRenamed("sum(Silver)", "Total_Silver")
    .withColumnRenamed("sum(Bronze)", "Total_Bronze")
    .withColumn("Total_Medals", col("Total_Gold") + col("Total_Silver") + col("Total_Bronze"))
    .orderBy(col("Total_Medals").desc())
)

# Show top 10 countries
top_countries_medals.show(10)


# COMMAND ----------

from pyspark.sql.functions import col

# Group by Rank and Team_NOC, sum all medals, and sort in descending order
top_country_discipline = (
    medals.groupBy("Rank", "Team/NOC")
    .sum("Gold", "Silver", "Bronze")
    .withColumnRenamed("sum(Gold)", "Total_Gold")
    .withColumnRenamed("sum(Silver)", "Total_Silver")
    .withColumnRenamed("sum(Bronze)", "Total_Bronze")
    .withColumn("Total_Medals", col("Total_Gold") + col("Total_Silver") + col("Total_Bronze"))
    .orderBy(col("Total_Medals").desc())
)

# Show top results
display(top_country_discipline)

# COMMAND ----------

from pyspark.sql.functions import sum

# Summing up total male and female athletes
total_gender = EntriesGender.select(
    sum("Male").alias("Total_Male_Athletes"),
    sum("Female").alias("Total_Female_Athletes")
)

# Show the result
total_gender.show()


# COMMAND ----------

# Calculate total athletes per discipline
top_discipline_athletes = (
    EntriesGender.withColumn("Total_Athletes", col("Male") + col("Female"))
    .groupBy("Discipline")
    .sum("Total_Athletes")
    .withColumnRenamed("sum(Total_Athletes)", "Total_Athletes")
    .orderBy(col("Total_Athletes").desc())
)

# Show top 10 sports
top_discipline_athletes.show(10)


# COMMAND ----------

