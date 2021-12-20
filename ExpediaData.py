# Databricks notebook source
storage_account_name = "expedia3"
client_id = "6f03e4a1-be1a-46f6-a33d-acdbab80f744"
tenant_id = "b41b72d0-4e9f-4c26-8a69-f949f367c91d"
client_secret = "YP37Q~sZHJ-hCTkXj1yi-.8xpV92dulFUspJK"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": f"{client_id}",
"fs.azure.account.oauth2.client.secret": f"{client_secret}",
"fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

container_name = "expediasource"
dbutils.fs.mount(
source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
mount_point = f"/mnt/{storage_account_name}/{container_name}",
extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/expedia3/expediasource")

# COMMAND ----------

from pyspark.sql.types import *

df=spark.read.format("avro").option("inferSchema","True").load("/mnt/expedia3/expediasource/*.avro")
df1= df.limit(20000)
df1.display()


# COMMAND ----------

# MAGIC %python
# MAGIC jdbcHostname="test-server25.database.windows.net"
# MAGIC jdbcDatabase="SQL_Database25"
# MAGIC jdbcusername="sqladm25"
# MAGIC jdbcpassword="sqladm.1*"
# MAGIC jdbcport="1433"
# MAGIC table="expedia1"
# MAGIC jdbcUrl="jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname,jdbcport,jdbcDatabase)

# COMMAND ----------

df1.write.mode("overwrite") \
.format("jdbc")  \
.option("url",jdbcUrl) \
.option("dbtable",table) \
.option("user", jdbcusername) \
.option("password", jdbcpassword) \
.option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
.save()

# COMMAND ----------

# MAGIC %sql
# MAGIC create table incrementalinfo(
# MAGIC tablename string,
# MAGIC start_time timestamp)
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into table incrementalinfo values('expedia1',cast('1900-01-01' as date))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from incrementalinfo;

# COMMAND ----------

import time
ts =time.time()
import datetime
ts1= spark.sql("select start_time from incrementalinfo where tablename= 'expedia1'").collect()[0][0].strftime('%Y-%m-%d %H:%M:%S')
query= "(select * from dbo.expedia1 where date_time>=cast('{0}' as datetime) ) z".format(ts1)
today1= datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
today1


# COMMAND ----------

# MAGIC %python
# MAGIC jdbcHostname="test-server25.database.windows.net"
# MAGIC jdbcDatabase="SQL_Database25"
# MAGIC jdbcusername="sqladm25"
# MAGIC jdbcpassword="sqladm.1*"
# MAGIC jdbcport="1433"
# MAGIC table="expedia1"
# MAGIC jdbcUrl="jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname,jdbcport,jdbcDatabase)
# MAGIC query1= """ update incrementalinfo
# MAGIC set start_time = cast('{0}' as timestamp)
# MAGIC where tablename='expedia1'""".format(today1)
# MAGIC incrDf= spark.read \
# MAGIC .format("jdbc") \
# MAGIC .option("url",jdbcUrl) \
# MAGIC .option("dbtable",query) \
# MAGIC .option("user", jdbcusername) \
# MAGIC .option("password", jdbcpassword) \
# MAGIC .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").option("numPartitions",10).option("partitionColumn","id") .option("lowerbound",10).option("upperbound",495944).load()
# MAGIC incrDf.count()

# COMMAND ----------

final=spark.read.format('jdbc').options(url='jdbc:sqlserver://workspace03.sql.azuresynapse.net:1433;database=SQLPool2', dbtable= 'finalsink.expedia', user='sqladminuser' ,password='sqladm.1*').load().limit(10)
final.count()

# COMMAND ----------

incrDf.registerTempTable("Incrementaldata")
final.registerTempTable("finaldata")


# COMMAND ----------

mergedData= spark.sql("""
select * from Incrementaldata
union all
select f.* from finaldata f anti join Incrementaldata i on i.id=f.id and i.hotel_id= f.hotel_id
""").limit(20000)
mergedData.count()

# COMMAND ----------

mergedData.write.mode("overwrite").option("truncate",True).format('jdbc').options(url='jdbc:sqlserver://workspace03.sql.azuresynapse.net:1433;database=SQLPool2',dbtable= 'finalsink.expedia',
user='sqladminuser' ,password='sqladm.1*').save()

# COMMAND ----------

spark.sql(query1)

# COMMAND ----------

spark.sql("select * from incrementalinfo").show()

# COMMAND ----------


