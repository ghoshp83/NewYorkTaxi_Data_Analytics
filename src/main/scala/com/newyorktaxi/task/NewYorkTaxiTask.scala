package com.newyorktaxi.task
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.expressions._
import com.newyorktaxi.utility.NewYorkTaxiConstants
import org.slf4j.LoggerFactory

object NewYorkTaxiTask {
  val log = LoggerFactory.getLogger("OpenSignalTaskLogger")
  def main(args: Array[String]): Unit = {

    log.info("Building the Spark Session for OpenSignal Task")
    val spark = SparkSession.builder()
      .appName("OpenSignal_Task")
      .config("spark.sql.parquet.compression.codec", "snappy")
      .getOrCreate()
    import spark.implicits._
    val landingDirectory = args(0)
    log.info("Reading the input data in parquet format")
    val parquetFileDF = spark.read.parquet(landingDirectory+"/ny_taxi_test_data/ny_taxi/*")
    log.info("Filtering the data with specific pickup datetime")
    val filteredDF = parquetFileDF.filter(
      to_date(col(NewYorkTaxiConstants.tpep_pickup_datetime_colName),NewYorkTaxiConstants.dateFormat) > "2015-01-15" &&
        to_date(col(NewYorkTaxiConstants.tpep_pickup_datetime_colName),NewYorkTaxiConstants.dateFormat) < "2015-02-15"
    )
    log.info("Reading the taxi ids who wants to be removed frm data store")
    val taxiIdCSV = spark.read.option("header",true).csv(landingDirectory+"/ny_taxi_test_data/ny_taxi_rtbf/rtbf_taxies.csv")
    log.info("Left Anti join to keep data only from left hand dataset of the join")
    val filteredTaxi = filteredDF.join(
      taxiIdCSV,
      Seq(NewYorkTaxiConstants.taxi_id_colName),
      "left_anti"
    )
    log.info("Reading taxi zones data")
    val taxizonesCSV = spark.read.option("header",true).csv(landingDirectory+"/ny_taxi_test_data/ny_zones/ny_taxi_zones.csv")
    log.info("Converting h3 index from base 10 to 16")
    val taxizonesHex = taxizonesCSV.withColumn(
      NewYorkTaxiConstants.h3_index_hex_colName,
      conv($"h3_index",10,16)
    )
    log.info("Geo Coded Pickup details in the dataset")
    val geoCodedPickUp = filteredTaxi.as("d1").join(
      taxizonesHex.as("d2"),$"d1.pickup_h3_index" === lower($"d2.h3_index_hex")
      )
      .select(
        $"d1.*",$"d2.zone".as(NewYorkTaxiConstants.pickup_zone_colName),$"d2.borough".as(NewYorkTaxiConstants.pickup_borough_colName)
      )
    log.info("Geo Coded Drop-off details in the dataset")
    val geoCodedDropOff = geoCodedPickUp.as("d1").join(
      taxizonesHex.as("d2"),$"d1.dropoff_h3_index" === lower($"d2.h3_index_hex")
      )
      .select($"d1.*",$"d2.zone".as(NewYorkTaxiConstants.dropoff_zone_colName),$"d2.borough".as(NewYorkTaxiConstants.dropoff_borough_colName))
    val rounded = geoCodedDropOff.withColumn(
      NewYorkTaxiConstants.trip_distance_round_colName, $"d1.trip_distance".cast(IntegerType)
    )

    log.info("Starting the Analytical activities")
    // Step 9 + 10
    val insight1 = rounded.groupBy(NewYorkTaxiConstants.trip_distance_round_colName)
      .agg(
        round(
          avg("d1.total_amount"),2
        ).as(NewYorkTaxiConstants.average_total_fare_alias)
        ,count(NewYorkTaxiConstants.trip_distance_round_colName).as(NewYorkTaxiConstants.number_of_trips_alias)
      )
      .orderBy($"trip_distance_round".asc)
      .withColumnRenamed(NewYorkTaxiConstants.trip_distance_round_colName,NewYorkTaxiConstants.trip_distance_colName)

    log.info("Creating csv file for Insight1 data")
    saveData(insight1,SaveMode.Overwrite,NewYorkTaxiConstants.analytics_type_one,landingDirectory)


    val task10 = rounded.groupBy(NewYorkTaxiConstants.trip_distance_round_colName)
      .agg(
        round(
          avg("d1.total_amount"),2
        ).as(NewYorkTaxiConstants.average_total_fare_alias),
        count("trip_distance_round").as(NewYorkTaxiConstants.number_of_trips_alias)
      )
      .orderBy($"trip_distance_round".desc)
      .withColumnRenamed(NewYorkTaxiConstants.trip_distance_round_colName,NewYorkTaxiConstants.trip_distance_colName)

    log.info("Upper limit of Trip Distance: "+task10.limit(1).show(false))
    log.info("Creating csv file for task10 data")
    saveData(task10,SaveMode.Overwrite,NewYorkTaxiConstants.analytics_task_ten,landingDirectory)


    // step 11
    log.info("Filtering the data to with null pickup and dropoff information")
    val filterNullData = rounded.filter($"pickup_zone".isNotNull || $"pickup_borough".isNotNull || $"dropoff_zone".isNotNull || $"dropoff_borough".isNotNull)

    // Insight 2 + 3
    val insight2 = filterNullData.groupBy(NewYorkTaxiConstants.pickup_zone_colName)
      .agg(
        count(NewYorkTaxiConstants.pickup_zone_colName)
          .as(NewYorkTaxiConstants.number_of_pickups_alias)
      )
      .withColumnRenamed(NewYorkTaxiConstants.pickup_zone_colName,NewYorkTaxiConstants.zone_colName)
    log.info("Creating csv file for Insight2 data")
    saveData(insight2,SaveMode.Overwrite,NewYorkTaxiConstants.analytics_type_two,landingDirectory)

    val insight3 = filterNullData.groupBy(NewYorkTaxiConstants.pickup_borough_colName)
      .agg(
        count(NewYorkTaxiConstants.pickup_borough_colName)
          .as(NewYorkTaxiConstants.number_of_pickups_alias)
      )
      .withColumnRenamed(NewYorkTaxiConstants.pickup_borough_colName,NewYorkTaxiConstants.borough_colName)
    log.info("Creating csv file for Insight3 data")
    saveData(insight3,SaveMode.Overwrite,NewYorkTaxiConstants.analytics_type_three,landingDirectory)

    // Insight 4 + 5
    val insight4 = filterNullData.groupBy(NewYorkTaxiConstants.dropoff_zone_colName)
      .agg(
        count(NewYorkTaxiConstants.dropoff_zone_colName).as(NewYorkTaxiConstants.number_of_dropoffs_alias)
        ,round(
          avg(NewYorkTaxiConstants.trip_distance_colName),2
        ).as(NewYorkTaxiConstants.average_trip_distance_alias)
        ,round(
          avg(NewYorkTaxiConstants.total_amount_colName),2
        ).as(NewYorkTaxiConstants.average_total_fare_alias)
      )
      .withColumnRenamed(NewYorkTaxiConstants.dropoff_zone_colName,NewYorkTaxiConstants.zone_colName)
    log.info("Creating csv file for Insight4 data")
    saveData(insight4,SaveMode.Overwrite,NewYorkTaxiConstants.analytics_type_four,landingDirectory)

    val insight5 = filterNullData.groupBy(NewYorkTaxiConstants.dropoff_borough_colName)
      .agg(
        count(NewYorkTaxiConstants.dropoff_borough_colName).as(NewYorkTaxiConstants.number_of_dropoffs_alias)
        ,round(
          avg(NewYorkTaxiConstants.trip_distance_colName),2
        ).as(NewYorkTaxiConstants.average_trip_distance_alias)
        ,round(
          avg(NewYorkTaxiConstants.total_amount_colName),2
        ).as(NewYorkTaxiConstants.average_total_fare_alias)
      )
      .withColumnRenamed("dropoff_zone","zone")
    log.info("Creating csv file for Insight5 data")
    saveData(insight5,SaveMode.Overwrite,NewYorkTaxiConstants.analytics_type_five,landingDirectory)




    // Insight 6
    val insight6Temp = filterNullData.groupBy(NewYorkTaxiConstants.pickup_zone_colName,NewYorkTaxiConstants.dropoff_zone_colName)
      .agg(
        count(NewYorkTaxiConstants.dropoff_zone_colName)
          .as(NewYorkTaxiConstants.number_of_dropoffs_alias)
      )
    val window = Window.partitionBy(NewYorkTaxiConstants.pickup_zone_colName)
      .orderBy(
        col(NewYorkTaxiConstants.number_of_dropoffs_colName).desc
      )
    val insight6 = insight6Temp.withColumn(NewYorkTaxiConstants.rank_colName,rank().over(window)).filter(col(NewYorkTaxiConstants.rank_colName) <= 5)
    log.info("Creating csv file for Insight6 data")
    saveData(insight6,SaveMode.Overwrite,NewYorkTaxiConstants.analytics_type_six,landingDirectory)


    // Insight 7
    val dataWithPickupDateHour = filterNullData.withColumn(
      NewYorkTaxiConstants.pickup_date_colName,to_date(col(NewYorkTaxiConstants.tpep_pickup_datetime_colName),NewYorkTaxiConstants.dateFormat)
    ).withColumn(
      NewYorkTaxiConstants.pickup_date_hour_colName,hour(col(NewYorkTaxiConstants.tpep_pickup_datetime_colName))
    )
    val insight7Temp = dataWithPickupDateHour.groupBy(NewYorkTaxiConstants.pickup_date_colName,NewYorkTaxiConstants.pickup_date_hour_colName)
      .agg(
        count(col(NewYorkTaxiConstants.pickup_date_hour_colName))
          .as(NewYorkTaxiConstants.no_of_trips_per_hour_alias)
      )
      .withColumnRenamed(NewYorkTaxiConstants.pickup_date_hour_colName,NewYorkTaxiConstants.hour_of_day_colName)
    val insight7 = insight7Temp.groupBy(
      NewYorkTaxiConstants.hour_of_day_colName
    ).agg(
      avg(NewYorkTaxiConstants.no_of_trips_per_hour_colName).as(NewYorkTaxiConstants.average_trips_alias)
    ).orderBy($"hour_of_day")
    log.info("Creating csv file for Insight7 data")
    saveData(insight7,SaveMode.Overwrite,NewYorkTaxiConstants.analytics_type_seven,landingDirectory)



  }

  def saveData(dataframe:DataFrame,saveMode: SaveMode, analyticsType:String, landingDirectory:String): Unit ={
    log.info("Start of saveData function")
    log.info("DataFrame: "+dataframe)
    log.info("SaveMode: "+saveMode)

    if(dataframe == null || saveMode == null || landingDirectory == null ) {
      return
    }
    dataframe
      .coalesce(1)
      .write.option("header",true)
      .mode(saveMode)
      .csv(landingDirectory+"/output/"+analyticsType)

    log.info("End of saveData function")
  }
}
