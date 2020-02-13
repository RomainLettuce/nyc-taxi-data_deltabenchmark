# nyc-taxi-data_deltabenchmark
This repo is mainly about querying nyc-taxi-data with delta lake which is an open source data lake satisfying ACID property.

Almost of uploaded files are from repo: https://github.com/toddwschneider/nyc-taxi-data

Also, data preprocessing and etl steps refer to a blog: https://tech.marksblogg.com/billion-nyc-taxi-rides-redshift.html

## Introduction

The project was measuring query times to benchmark delta lake with my own db.

Originally, many data scientists used nyc-taxi-data dataset which has about 2.45 billion trip datas or portion of it
to measure executuion time of various queries.

However my server environment temporarily didn't serve proxy/vpn so, I needed to work with small portions of this original dataset and preprocess the small dataset on my local pc then, send it to server.

I repeated this work several times and I could get 1.1 billion rows in my server environment and use it to measure queries.

This repo explains some changes of build-up files and preprocessing steps and finally measuring methods I used.

## Dependency

* **local PC)**

postgresql: **12.1**

postgis: **3.0.0**

* **Server)**

spark: **2.4.4**

hadoop: **2.8.5**

delta-core: **delta-core_2.12-0.4.0.jar**

zeppelin: **0.8.2**

## 1. Preprocessing

**1. Download raw data**

**download_raw_data.sh** automatically downloads every trips by wget all urls from **setup_files/raw_data_urls.txt**

However, in my case, the local pc had only about 50GB space available so, I commented out all urls except 2~4 target urls to download small portion of raw dataset. You can also make new txt file that contains only files that you want to download. Feel free to modify methods.

After comment out all urls except your target urls, execute

	./download_raw_data.sh

downloaded low data files saved in **data** directory.

Yellow taxi trip data files of **2010-02 & 2010-03** contains **bad row** which does not fit in next steps so, you should remove that rows after you download yellow trip data of **2010-02 & 2010-03** using following command.

	./remove_bad_rows.sh

**2. initialize db**

**initialize_database.sh** automatically creates db on postgresql and makes some tables by using exist datasets **data/central_park_weather.csv**, **data/fhv_bases.csv** and directory **shapefiles**

In my case, original **initialize_database.sh** made some errors because it uses some functions from postgis to handle nested attributes but, original sh file did not import postgis.

Therefore, I added following two lines after create schema.

	psql nyc-taxi-data -c "CREATE EXTENSION postgis;"
	psql nyc-taxi-data -c "SELECT PostGIS_Version();"

Execute following line to initialize db and some tables in postgresql.

	./initialize_database.sh

**3. import data to postgresql**

**./import_trip_data.sh** automatically import data files in **data** directory to postgresql(db name is nyc-taxi-data).

Execute following command to import data to postgresql.

	./import_trip_data.sh

If the data files successfully imported to postgresql, there should be **'trips'** table and some rows in that table after you use following commands.

	psql nyc-taxi-data

then,

	SELECT relname table_name,
  	lpad(to_char(reltuples, 'FM9,999,999,999'), 13) row_count
	FROM pg_class
	LEFT JOIN pg_namespace
		ON (pg_namespace.oid = pg_class.relnamespace)
	WHERE nspname NOT IN ('pg_catalog', 'information_schema')
	AND relkind = 'r'
	ORDER BY reltuples DESC;

<img src="image3.png" width="40%" height="40%" title="" alt=""></img>

**4. Preprocessing imported data**

After import trip datas into postgresql, you should merge this table with some previouly exist tables and set or change some column name.

You can do this work after connect to the db **nyc-taxi-data**

	psql nyc-taxi-data

If you connect to the db, you can preprocess and export the imported data with following command. You can also see this command line in **format.txt**

	COPY (
		SELECT trips.cab_type_id,
		 trips.vendor_id,
		 trips.pickup_datetime,
		 trips.dropoff_datetime,
		 trips.store_and_fwd_flag,
		 trips.rate_code_id,
		 trips.pickup_longitude,
		 trips.pickup_latitude,
		 trips.dropoff_longitude,
		 trips.dropoff_latitude,
		 trips.passenger_count,
		 trips.trip_distance,
		 trips.fare_amount,
		 trips.extra,
		 trips.mta_tax,
		 trips.tip_amount,
		 trips.tolls_amount,
		 trips.improvement_surcharge,
		 trips.total_amount,
		 trips.payment_type,
		 trips.trip_type,

		 cab_types.type cab_type,

		 weather.precipitation rain,
		 weather.snow_depth,
		 weather.snowfall,
		 weather.max_temperature max_temp,
		 weather.min_temperature min_temp,
		 weather.average_wind_speed wind,

		 pick_up.gid pickup_nyct2010_gid,
		 pick_up.ctlabel pickup_ctlabel,
		 pick_up.borocode pickup_borocode,
		 pick_up.boroname pickup_boroname,
		 pick_up.ct2010 pickup_t2010,
		 pick_up.boroct2010 pickup_boroct2010,
		 pick_up.cdeligibil pickup_cdeligibil,
		 pick_up.ntacode pickup_ntacode,
		 pick_up.ntaname pickup_ntaname,
		 pick_up.puma pickup_puma,

		 drop_off.gid dropoff_nyct2010_gid,
		 drop_off.ctlabel dropoff_ctlabel,
		 drop_off.borocode dropoff_borocode,
		 drop_off.boroname dropoff_boroname,
		 drop_off.ct2010 dropoff_ct2010,
		 drop_off.boroct2010 dropoff_boroct2010,
		 drop_off.cdeligibil dropoff_cdeligibil,
		 drop_off.ntacode dropoff_ntacode,
		 drop_off.ntaname dropoff_ntaname,
		 drop_off.puma dropoff_puma
			FROM trips
			LEFT JOIN cab_types
					ON trips.cab_type_id = cab_types.id
			LEFT JOIN central_park_weather_observations weather
					ON weather.date = trips.pickup_datetime::date
			LEFT JOIN nyct2010 pick_up
					ON pick_up.gid = trips.pickup_nyct2010_gid
			LEFT JOIN nyct2010 drop_off
					ON drop_off.gid = trips.dropoff_nyct2010_gid
	) TO PROGRAM
		'gzip > /Users/mf839-033/nyc-taxi-data_deltabenchmark/trips/your_filename.csv.gz'
		WITH CSV;

You can change path of the exported file on second from last line.

**ps) I refer to the blog that I linked to write above command. The original command of the blog did not match with some column name of weather csv file. Therefore, I accordingly changed some column name to finish merging tables successfully.**

**5. Truncate table**

If you are planning repeat above process for many partitions, you should truncate trips table after export the trip datas.

If you don't, trips data will be able to have duplicates and it can also cause lack of memory later.

Therefore, use following command to truncate used tables and prevent above problems.

	TRUNCATE TABLE trips;

**6. Send csv to server**

Finally, send processed csv file to the server that save datas in hadoop dfs.

Use scp command to csv file. Following is example.

	scp /Your/processed/csv_file/path server_username@server_ip:/Target/file/path

## 2. Hadoop & Spark Environment

Hadoop and Spark environment set up is up to you.

Be sure to use the most recently updated version of pyspark by using following command. It is essential to use delta core in next step.

	pip install --upgrade pyspark

In my case, my server couldn't use internet so, I first download spark at the local mac pc and used pip upgrade command to update pyspark.

Then, I sent whole spark directory to Linux server.

It has no problem because spark for Unix shows same behaviors on both mac and Linux.

If you can use internet in your server, just use the most simplest way to set up hadoop and spark.

## 3. Zeppelin set up

First, download zeppelin and set up essential configurations such as zeppelin server url port or spark home etc)

Then, connect to zeppelin server port url and log in with your account. Now you can change your zeppelin spark interpreter.

Add your delta core jar file's path to jars.

Also, set up your spark Master and executor cores or instances.

![Alt text](image1.png)

## 4. Measuring

**1. Load files to hadoop dfs**
Turn on your hadoop, yarn, spark, zeppelin.

	start-dfs.sh
	start-yarn.sh
	start-all.sh
	zeppelin-daemon.sh start

**(You should set up all the path correctly.)

Create your data storage in your hadoop dfs.

	hadoop fs -mkdir nyc-taxi-data

Unzip your csv.gz files and put the csv results to your hadoop distributed file system.

	hadoop fs -put /your/file/path.csv.gz nyc-taxi-data

Now, you can load dataframe with your data files on zeppelin from your server's hadoop dfs.

**2. Load dataframe on zeppelin notebook**
Make new spark project on zeppelin and write following paragraph to load dataframe and set column names and column types. The paragraph finally convert the df to delta table.

	%spark.pyspark
	from pyspark.sql.functions import *
	from pyspark.sql.types import *
	df = spark.read.csv(“nyc-taxi-data/*.csv”)
	df = df.withColumn(“id”, col(“_c0”).cast(IntegerType())).drop(“_c0")
	df = df.withColumnRenamed(“_c1”, “vendor_id”)
	df = df.withColumn(“pickup_datetime”, col(“_c2").cast(TimestampType())).drop(“_c2”)
	df = df.withColumn(“dropoff_datetime”, col(“_c3”).cast(TimestampType())).drop(“_c3")
	df = df.withColumnRenamed(“_c4”, “store_and_fwd_flag”)
	df = df.withColumn(“rate_code_id”, col(“_c5").cast(IntegerType())).drop(“_c5”)
	df = df.withColumn(“pickup_longitude”, col(“_c6”).cast(DoubleType())).drop(“_c6")
	df = df.withColumn(“pickup_latitude”, col(“_c7").cast(DoubleType())).drop(“_c7”)
	df = df.withColumn(“dropoff_longitude”, col(“_c8”).cast(DoubleType())).drop(“_c8")
	df = df.withColumn(“dropoff_latitude”, col(“_c9").cast(DoubleType())).drop(“_c9”)
	df = df.withColumn(“passenger_count”, col(“_c10”).cast(IntegerType())).drop(“_c10")
	df = df.withColumn(“trip_distance”, col(“_c11").cast(DoubleType())).drop(“_c11”)
	df = df.withColumn(“fare_amount”, col(“_c12”).cast(DoubleType())).drop(“_c12")
	df = df.withColumn(“extra”, col(“_c13").cast(DoubleType())).drop(“_c13”)
	df = df.withColumn(“mta_tax”, col(“_c14”).cast(DoubleType())).drop(“_c14")
	df = df.withColumn(“tip_amount”, col(“_c15").cast(DoubleType())).drop(“_c15”)
	df = df.withColumn(“tolls_amount”, col(“_c16”).cast(DoubleType())).drop(“_c16")
	df = df.withColumn(“improvement_surcharge”, col(“_c17").cast(DoubleType())).drop(“_c17”)
	df = df.withColumn(“total_amount”, col(“_c18”).cast(DoubleType())).drop(“_c18")
	df = df.withColumnRenamed(“_c19”, “payment_type”)
	df = df.withColumn(“trip_type”, col(“_c20").cast(IntegerType())).drop(“_c20”)
	df = df.withColumnRenamed(“_c21", “cab_type”)
	df = df.withColumn(“rain”, col(“_c22”).cast(IntegerType())).drop(“_c22")
	df = df.withColumn(“snow_depth”, col(“_c23").cast(IntegerType())).drop(“_c23”)
	df = df.withColumn(“snowfall”, col(“_c24”).cast(IntegerType())).drop(“_c24")
	df = df.withColumn(“max_temp”, col(“_c25").cast(IntegerType())).drop(“_c25”)
	df = df.withColumn(“min_temp”, col(“_c26”).cast(IntegerType())).drop(“_c26")
	df = df.withColumn(“wind”, col(“_c27").cast(IntegerType())).drop(“_c27”)
	df = df.withColumn(“pickup_nyct2010_gid”, col(“_c28”).cast(IntegerType())).drop(“_c28")
	df = df.withColumnRenamed(“_c29”, “pickup_ctlabel”)
	df = df.withColumn(“pickup_borocode”, col(“_c30").cast(IntegerType())).drop(“_c30”)
	df = df.withColumnRenamed(“_c31", “pickup_boroname”)
	df = df.withColumnRenamed(“_c32", “pickup_t2010”)
	df = df.withColumnRenamed(“_c33", “pickup_boroct2010”)
	df = df.withColumnRenamed(“_c34", “pickup_cdeligibil”)
	df = df.withColumnRenamed(“_c35", “pickup_ntacode”)
	df = df.withColumnRenamed(“_c36", “pickup_ntaname”)
	df = df.withColumnRenamed(“_c37", “pickup_puma”)
	df = df.withColumn(“dropoff_nyct2010_gid”, col(“_c38”).cast(IntegerType())).drop(“_c38")
	df = df.withColumnRenamed(“_c39”, “dropoff_ctlabel”)
	df = df.withColumn(“dropoff_borocode”, col(“_c40").cast(IntegerType())).drop(“_c40”)
	df = df.withColumnRenamed(“_c41", “dropoff_boroname”)
	df = df.withColumnRenamed(“_c42", “dropoff_t2010”)
	df = df.withColumnRenamed(“_c43", “dropoff_boroct2010”)
	df = df.withColumnRenamed(“_c44", “dropoff_cdeligibil”)
	df = df.withColumnRenamed(“_c45", “dropoff_ntacode”)
	df = df.withColumnRenamed(“_c46", “dropoff_ntaname”)
	df = df.withColumnRenamed(“_c47", “dropoff_puma”)
	df.write.format(“delta”).mode(“append”).save(“nyc-taxi-data/trips”)

Then, make a new paragraph and create a table using delta from your delta table which was made in previous step.


	%spark.sql

	CREATE TABLE trips
		USING DELTA
		LOCATION 'nyc-taxi-data/trips'

**3. Try queries**
Now you can use this table to query. For example, I made following 4 queries which are used in the blog that I refered.

**Query1**

	%spark.sql
	
	SELECT cab_type,
		count(*)
	FROM trips
	GROUP BY cab_type

**Query2**

	%spark.sql
	
	SELECT passenger_count,
		avg(total_amount)
	FROM trips
	GROUP BY passenger_count

**Query3**

	%spark.sql
	
	SELECT passenger_count,
		year(pickup_datetime),
		count(*)
	FROM trips
	GROUP BY passenger_count,
		year(pickup_datetime)

**Query4**

	%spark.sql
	
	SELECT passenger_count,
		year(pickup_datetime) trip_year,
		round(trip_distance),
		count(*) trips
	FROM trips
	GROUP BY passenger_count,
		year(pickup_datetime),
		round(trip_distance)
	ORDER BY trip_year,
		trips desc

Each query is written in a separated paragraph and you can see the result of query with various format by using zeppelin's tools.


**4. Measure execution time**
You can check each query's sub tasks and execution time to finish each tasks in spark jobs url. (In my case http://my_ip:4044)

Following picture shows interface of spark jobs web site. You can easily add all tasks' execution time and the result is the taken time to query.

![Alt text](image2.png)

For example, my example query 1 had **6 tasks** and total taken time was **0.039 + 0.082 + 0.035 + 0.023 + 3 + 0.2 = 3.379 (sec)**

## 5. Comment
Almost of preprocessing processes refer to commented git hub repo and blog post. Thanks to both guys. Those sources also give you many insights or help.

If you have some trouble during set up hadoop/spark/zeppelin, contact me with an E-mail or issue tab. I will give you help as much as I can.

## 6. Contact

Send E-mail to wsm723@kaist.ac.kr or contact with issue tab
