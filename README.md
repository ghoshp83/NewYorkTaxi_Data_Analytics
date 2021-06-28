# NewYorkTaxi_Data_Analytics
Analytics on NewYork Taxi Data

# Source Data
It is present in ~/source directory of this repository

# Output Data
It is present in ~/output directory of this repository

# Problem Statement
Let's gain some insight from a medium-biggish dataset. We will be looking at New York taxi data that are sourced from https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page.
Let's take 2 months data(converted as parquet) for this activity. 

Data details 
-------------------------
ny_taxi schema : 
```
column name	type	notes
vendor_id	int	
tpep_pickup_datetime	timestamp	
tpep_dropoff_datetime	timestamp	
passenger_count	int	
trip_distance	double	
pickup_longitude	double	
pickup_latitude	double	
rate_code_id	int	
store_and_forward	string	
dropoff_longitude	double	
dropoff_latitude	double	
payment_type	int	
fare_amount	double	
extra	double	
mta_tax	double	
tip_amount	double	
tolls_amount	double	
improvement_surcharge	double	
total_amount	double	
pickup_h3_index	string	h3 index in base 16 (HEX) format
dropoff_h3_index	string	h3 index in base 16 (HEX) format
taxi_id	int	
```

ny_zones schema:
```
column name	notes
h3_index	h3 index in base 10 (Decimal) format
zone	
borough	
location_id
```

ny_taxi_rtbf schema:
```
column name	notes
taxi_id	ids of taxi drivers who would like to be forgotten
```

Activities
-----------------------------
1. Filter the ny_taxi data between the dates ```2015-01-15``` to ```2015-02-15``` using the ```tpep_pickup_datetime``` column.
2. Filter right to be forgotten ```taxi_ids```. Remove all rows that have a taxi_id that is in the ny_taxi_rtbf list.
3. Load geocoding data ```ny_zones```
4. Using the geocoding data (```ny_zones```) and the appropriate index column in the ```ny_taxi``` data, geocode each pickup location with zone and borough. 
   We would like 2 new columns: ```pickup_zone``` and ```pickup_borough```.
5. Using the geocoding data (```ny_zones```) and the appropriate index column in the ```ny_taxi``` data, geocode each dropoff location with zone and borough.                        We would like 2 new columns: ```dropoff_zone``` and ```dropoff_borough```.
6. Calculate the average total fare for each ```trip_distance``` (trip distance bin size of 1, or rounded to 0 decimal places) and the number of trips.                              Order the output by trip_distance. Write the output as a single csv with headers. The resulting output should have 3 columns: ```trip_distance```, ```average_total_fare```
   and ```number_of_trips```.
7. Looking at the output of step 6, decide what would be an appropriate upper limit of ```trip_distance``` and rerun with this filtered.
8. Filter rows if any of the columns: ```pickup_zone```, ```pickup_borough```, ```dropoff_zone``` and ```dropoff_borough``` are null.
9. Total number of pickups in each zone. Write the output as a single csv with headers. The resulting output should have 2 columns: ```zone``` and ```number_of_pickups```.
10. Total number of pickups in each borough. Write the output as a single csv with headers. The resulting output should have 2 columns: ```borough``` and ```number_of_pickups```.
11. Total number of dropoffs, average total cost and average distance in each zone. Write the output as a single csv with headers. The resulting output should have 4 columns:       ```zone```, ```number_of_dropoffs```, ```average_total_fare``` and ```average_trip_distance```.
12. Total number of dropoffs, average total cost and average distance in each borough. Write the output as a single csv with headers. The resulting output should have 4 columns:     ```borough```, ```number_of_dropoffs```, ```average_total_fare``` and ```average_trip_distance```.
13. For each pickup zone calculate the top 5 dropoff zones ranked by number of trips. Write output as a single csv with headers. The resulting output should have 4 columns:         ```pickup_zone```, ```dropoff_zone```, ```number_of_dropoffs``` and ```rank```.
14. Calculate the number of trips for each date -> pickup hour, (using ```tpep_pickup_datetime```), then calculate the average number of trips by hour of day. The resulting         output should have 2 columns: ```hour_of_day``` and ```average_trips```.


Execution
---------------------
Local Mode : 
```
<SPARK_HOME>/bin/spark-submit --class com.newyorktaxi.task.NewYorkTaxiTask --master local[8] --conf spark.driver.host=127.0.0.1 --driver-memory 8g <location_of_jar_file>/NewYorkTaxiAnalytics-1.0-1.jar <location_of_the_source_data>. 
```
Example: 
```
~/spark-3.1.2-bin-hadoop3.2/bin/spark-submit --class com.newyorktaxi.task.NewYorkTaxiTask --master local[8] --conf spark.driver.host=127.0.0.1 --driver-memory 8g ~/jars/NewYorkTaxiAnalytics-1.0-1.jar ~/source
```

Environment
---------------------
Spark 3.1.2
Scala 2.12
Java 1.8

Main Class
---------------------
com.newyorktaxi.task.NewYorkTaxiTask

Approach
---------------------
Spark SQL DataFrame API using Scala
