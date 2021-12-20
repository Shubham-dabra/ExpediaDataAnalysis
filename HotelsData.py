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

container_name = "hotel-11"
dbutils.fs.unmount(mount_point = f"/mnt/{storage_account_name}/hotel-11")
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/{container_name}",
  extra_configs = configs)


# COMMAND ----------

dbutils.fs.ls("/mnt/expedia3/hotel-11")

# COMMAND ----------

file_location = "/mnt/expedia3/hotel-11/*.csv"

raw = spark.read.csv(file_location,inferSchema = True, header = True)
raw.show(10)

# COMMAND ----------

raw.createOrReplaceTempView("Hotel_data")
count = spark.sql(" select count(*) as count from Hotel_data ")
count.show()

# COMMAND ----------

invalid_data = spark.sql("select * from Hotel_data where Latitude is null or Longitude is null or Latitude rlike 'NA' or Longitude rlike 'NA' ")
invalid_data.show()
invalid_data.count()

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install pygeohash

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install opencage

# COMMAND ----------

from opencage.geocoder import OpenCageGeocode
import pygeohash as geohash

key = '77fba6afd5864138a5f575eb3389cb4c'
geocoder = OpenCageGeocode(key)

def geo_lat_lon(name, address, city, country, tag):
    value = geocoder.geocode("{},{},{},{}".format(name,address,city,country))
    return (value[0]['geometry'][tag])

lat_lon = udf(geo_lat_lon)


# COMMAND ----------

def hashfunc(lat,long) : 
    if lat is None or long is None:
      lat = 0.0
      long =0.0
    return (geohash.encode(lat,long,precision=5))

geohash_udf = udf(hashfunc)

# COMMAND ----------

from pyspark.sql.functions import col,lit
modified_data = invalid_data.withColumn("lat", lat_lon(col("name"),col("address"),col("city"),col("country"),lit("lat"))) \
                            .withColumn("long", lat_lon(col("name"),col("address"),col("city"),col("country"),lit("lng"))) \
                            .drop('Latitude','Longitude')
                       
modified_data.show()
                    

# COMMAND ----------

hotel_valid_data = raw.subtract(invalid_data)
hotel_valid_data.count()

# COMMAND ----------

Hotel_data = hotel_valid_data.union(modified_data)
display(Hotel_data)

# COMMAND ----------

Hotel_data.count()

# COMMAND ----------

Hotel_final = Hotel_data.withColumn("geohash",geohash_udf(col("latitude").cast("float"),col("longitude").cast("float")))
display(Hotel_final)

# COMMAND ----------

Hotel_final.write.format('jdbc').options(url='jdbc:sqlserver://workspace03.sql.azuresynapse.net:1433;database=SQLPool2',dbtable = 'Hotel_Table',user = 'sqladminuser', password = 'sqladm.1*').mode('append').save()

