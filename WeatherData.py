# Databricks notebook source
#containerName = "weather-data"
#storageAccountName = "myaccount1996"
#sas = "?sv=2020-08-04&ss=bfqt&srt=sco&sp=rwdlacupx&se=2021-12-15T16:26:22Z&st=2021-12-14T08:26:22Z&spr=https&sig=kVUx3SQ8nbYeUUmqVOX9a%2B8OF1T9w6sLCbV%2BT2fjotc%3D"
#config = "fs.azure.sas." + containerName+ "." + storageAccountName + ".blob.core.windows.net"

# COMMAND ----------

#dbutils.fs.mount(
#  source = "wasbs://weather-data@myaccount1996.blob.core.windows.net/weather-data",
#  mount_point = "/mnt/myaccount1996/weather-data",
#  extra_configs = map("fs.azure.account.key.myaccount1996.blob.core.windows.net" -> sas))



# COMMAND ----------

storage_account_name = "myaccount1996"
client_id = "13246d0c-3bea-401b-be7e-bbcb120ce672"
tenant_id = "7857aa44-5e72-40ff-a58f-50ef9e2aca63"
client_secret = "ak57Q~prIUiE5y4kRnPjGgfYZ-wCUSxruUquM"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{client_id}",
          "fs.azure.account.oauth2.client.secret": f"{client_secret}",
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

container_name = "weather-new-data"
dbutils.fs.unmount(mount_point = f"/mnt/{storage_account_name}/weather-new-data")
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/{container_name}",
  extra_configs = configs)

# COMMAND ----------

file_location = "/mnt/myaccount1996/weather-new-data/"

r = spark.read.parquet(file_location,inferSchema = True, header = True)
raw=r.sample(0.01)

# COMMAND ----------

raw.schema

# COMMAND ----------

from pyspark.sql.functions import *
raw_new = raw.withColumn('key',to_json(struct([raw[col] for col in raw.columns]))).select('key')
display(raw_new)

# COMMAND ----------

raw_df = raw_new.withColumnRenamed('key','body')

# COMMAND ----------

raw_df.show(100,False)

# COMMAND ----------

connection = "Endpoint=sb://event-hub11.servicebus.windows.net/;SharedAccessKeyName=policy_2;SharedAccessKey=zLknetn1UUB9Nu8wN1zjUsfO+od3y+Ty9CpRVZ9q824=;EntityPath=event_hub"
conf =  {}
conf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection)

# COMMAND ----------

raw_df.select('body').write.format("eventhubs").options(**conf).save()

# COMMAND ----------

import json
start_event_position = {
    "offset" : "-1",
    "seqNo" : -1,
    "enqueuedTime": None,
    "isInclusive": True
    
}
conf_1 = {}
conf_1["eventhubs.connectionString"] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection)
conf_1["eventhubs.startingPosition"] = json.dumps(start_event_position)

# COMMAND ----------

weather = spark.read.format("eventhubs").options(**conf_1).load()

# COMMAND ----------

from pyspark.sql.types import *
import  pyspark.sql.functions as F
schema = StructType([
StructField("lng",DoubleType()),
StructField("lat",DoubleType()),
StructField("avg_tmpr_f",DoubleType()),
StructField("avg_tmpr_c",DoubleType()),
StructField("wthr_date",StringType()),
StructField("year",IntegerType()),
StructField("month",IntegerType()),
StructField("day",IntegerType())])



# COMMAND ----------

weather_df = weather.select(F.from_json(F.col("body").cast("string"),schema).alias("data")).select("data.*")
weather_df.show(10,False)


# COMMAND ----------

weather_df.count()

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install pygeohash

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install opencage

# COMMAND ----------

import pygeohash as geohash
def hashfunc(lat,long) : 
    if lat is None or long is None:
      lat = 0.0
      long =0.0
    return (geohash.encode(lat,long,precision=5))

geohash_udf = udf(hashfunc)

# COMMAND ----------

weather_final = weather_df.withColumn("geohash_weather",geohash_udf(col("lat").cast("float"),col("lng").cast("float")))
display(weather_final)

# COMMAND ----------

#Hotel_df = spark.read.format('jdbc').options(url='jdbc:sqlserver://synapse1196.sql.azuresynapse.net:1433;database=snynapsepool',dbtable = 'Hotel_Table',user = 'sqladminuser@synapse1196', password = 'Sagar@2021').load()


# COMMAND ----------

display(Hotel_df)

# COMMAND ----------

condition = [(Hotel_df['geohash'] == weather_final['geohash_weather']) \
            | (Hotel_df['geohash'][1:4] == weather_final['geohash_weather'][1:4]) \
            | (Hotel_df['geohash'][1:3] == weather_final['geohash_weather'][1:3])
            ]

# COMMAND ----------

hotel_weather_df = Hotel_df.join(weather_final, condition, "inner")
hotel_weather_df.show(100,False)

# COMMAND ----------

hotel_weather_precision = hotel_weather_df.withColumn('precision', \
                                                      when(col("geohash") == col("geohash_weather"),lit(5)) \
                                                     .when(col("geohash")[1:4] == col("geohash_weather")[1:4],lit(4))
                                                     .when(col("geohash")[1:3] == col("geohash_weather")[1:3],lit(3))
                                                     )
hotel_weather_precision.show(100,False)

# COMMAND ----------

file_location = "/mnt/myaccount1996/weather-new-data/expedia"

ex_raw = spark.read.format("avro").load(file_location)
ex_raw_1=ex_raw.sample(0.25)
display(ex_raw_1)

# COMMAND ----------

from pyspark.sql.window import Window
expedia_df = ex_raw_1.withColumn('idle_days',datediff(col('srch_ci'),lag(col('srch_ci'),1).over(Window.partitionBy(col('hotel_id')).orderBy(col('srch_ci')))))
expedia_df = expedia_df.fillna(0,("idle_days"))
display(expedia_df)

# COMMAND ----------

valid_booking = expedia_df.filter(((col('idle_days') >= 2) & (col('idle_days') <= 30)))
display(valid_booking)

# COMMAND ----------

invalid_booking = expedia_df.subtract(valid_booking)
display(invalid_booking)

# COMMAND ----------

#Hotel_df = spark.read.format('jdbc').options(url='jdbc:sqlserver://synapse1196.sql.azuresynapse.net:1433;database=snynapsepool',dbtable = 'Hotel_Table',user = 'sqladminuser@synapse1196', password = 'Sagar@2021').load()


# COMMAND ----------

expedia_hotel_df = valid_booking.join(Hotel_df,valid_booking['hotel_id'] == Hotel_df['id'],'left')
expedia_hotel_df.show(100,False)

# COMMAND ----------

booking_country = expedia_hotel_df.groupBy('country').count()
display(booking_country)

# COMMAND ----------

booking_country = expedia_hotel_df.groupBy('city').count()
display(booking_country)

# COMMAND ----------

valid_booking = valid_booking.withColumnRenamed('id', 'booking_id')
combine_df = valid_booking.join(hotel_weather_precision , valid_booking['hotel_id'] == hotel_weather_precision['id'])
display(combine_df)

# COMMAND ----------

combine_avg_temp = combine_df.filter(col('avg_tmpr_c') > 0.0)
combine_avg_temp.show(100,False)

# COMMAND ----------

stay_duration = combine_avg_temp.withColumn('stay_duration(in days)', datediff(col('srch_co'),col('srch_ci')))
display(stay_duration)

# COMMAND ----------

from pyspark.sql.functions import lit
prefered_stay = stay_duration.withColumn('Customer Preference',when(((col('stay_duration(in days)').isNull()) | (col('stay_duration(in days)') <= 0 )| (col('stay_duration(in days)') >= 30)),lit("Erroneous data")) \
                                         .when(col('stay_duration(in days)') == 1, lit("Short Stay")) \
                                         .when(((col('stay_duration(in days)') >= 2) | (col('stay_duration(in days)') <= 7)),lit("Standard Stay")) \
                                        .when(((col('stay_duration(in days)') >= 8) | (col('stay_duration(in days)') <= 14)),lit("Standard Extended Stay")) \
                                        .when(((col('stay_duration(in days)') >= 15) | (col('stay_duration(in days)') <= 29)),lit("Long Stay")))



# COMMAND ----------

display(prefered_stay)

# COMMAND ----------

prefered_stay_max_count = prefered_stay.groupBy("hotel_id","Customer Preference") \
                                       .agg(count("hotel_id").alias("Hotel_max_count")).sort(desc("Hotel_max_count")).limit(1)
display(prefered_stay_max_count)

# COMMAND ----------

final = stay_duration.withColumn('with_children', when(col('srch_children_cnt') > 0,lit("Yes")).otherwise(lit("No")))
display(final)

# COMMAND ----------


