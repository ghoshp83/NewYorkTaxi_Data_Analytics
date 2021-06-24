package com.opensignal.task
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.expressions._
import com.opensignal.utility.OpenSignalConstants
import org.slf4j.LoggerFactory

object OpenSignalTask {
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
      to_date(col(OpenSignalConstants.tpep_pickup_datetime_colName),OpenSignalConstants.dateFormat) > "2015-01-15" &&
        to_date(col(OpenSignalConstants.tpep_pickup_datetime_colName),OpenSignalConstants.dateFormat) < "2015-02-15"
    )
    log.info("Reading the taxi ids who wants to be removed frm data store")
    val taxiIdCSV = spark.read.option("header",true).csv(landingDirectory+"/ny_taxi_test_data/ny_taxi_rtbf/rtbf_taxies.csv")
    log.info("Left Anti join to keep data only from left hand dataset of the join")
    val filteredTaxi = filteredDF.join(
      taxiIdCSV,
      Seq(OpenSignalConstants.taxi_id_colName),
      "left_anti"
    )
    log.info("Reading taxi zones data")
    val taxizonesCSV = spark.read.option("header",true).csv(landingDirectory+"/ny_taxi_test_data/ny_zones/ny_taxi_zones.csv")
    log.info("Converting h3 index from base 10 to 16")
    val taxizonesHex = taxizonesCSV.withColumn(
      OpenSignalConstants.h3_index_hex_colName,
      conv($"h3_index",10,16)
    )
    log.info("Geo Coded Pickup details in the dataset")
    val geoCodedPickUp = filteredTaxi.as("d1").join(
      taxizonesHex.as("d2"),$"d1.pickup_h3_index" === lower($"d2.h3_index_hex")
      )
      .select(
        $"d1.*",$"d2.zone".as(OpenSignalConstants.pickup_zone_colName),$"d2.borough".as(OpenSignalConstants.pickup_borough_colName)
      )
    log.info("Geo Coded Drop-off details in the dataset")
    val geoCodedDropOff = geoCodedPickUp.as("d1").join(
      taxizonesHex.as("d2"),$"d1.dropoff_h3_index" === lower($"d2.h3_index_hex")
      )
      .select($"d1.*",$"d2.zone".as(OpenSignalConstants.dropoff_zone_colName),$"d2.borough".as(OpenSignalConstants.dropoff_borough_colName))
    val rounded = geoCodedDropOff.withColumn(
      OpenSignalConstants.trip_distance_round_colName, $"d1.trip_distance".cast(IntegerType)
    )

    log.info("Starting the Analytical activities")
    // Step 9 + 10
    val insight1 = rounded.groupBy(OpenSignalConstants.trip_distance_round_colName)
      .agg(
        round(
          avg("d1.total_amount"),2
        ).as(OpenSignalConstants.average_total_fare_alias)
        ,count(OpenSignalConstants.trip_distance_round_colName).as(OpenSignalConstants.number_of_trips_alias)
      )
      .orderBy($"trip_distance_round".asc)
      .withColumnRenamed(OpenSignalConstants.trip_distance_round_colName,OpenSignalConstants.trip_distance_colName)

    log.info("Creating csv file for Insight1 data")
    saveData(insight1,SaveMode.Overwrite,OpenSignalConstants.analytics_type_one,landingDirectory)


    val task10 = rounded.groupBy(OpenSignalConstants.trip_distance_round_colName)
      .agg(
        round(
          avg("d1.total_amount"),2
        ).as(OpenSignalConstants.average_total_fare_alias),
        count("trip_distance_round").as(OpenSignalConstants.number_of_trips_alias)
      )
      .orderBy($"trip_distance_round".desc)
      .withColumnRenamed(OpenSignalConstants.trip_distance_round_colName,OpenSignalConstants.trip_distance_colName)

    log.info("Upper limit of Trip Distance: "+task10.limit(1).show(false))
    log.info("Creating csv file for task10 data")
    saveData(task10,SaveMode.Overwrite,OpenSignalConstants.analytics_task_ten,landingDirectory)


    // step 11
    log.info("Filtering the data to with null pickup and dropoff information")
    val filterNullData = rounded.filter($"pickup_zone".isNotNull || $"pickup_borough".isNotNull || $"dropoff_zone".isNotNull || $"dropoff_borough".isNotNull)

    // Insight 2 + 3
    val insight2 = filterNullData.groupBy(OpenSignalConstants.pickup_zone_colName)
      .agg(
        count(OpenSignalConstants.pickup_zone_colName)
          .as(OpenSignalConstants.number_of_pickups_alias)
      )
      .withColumnRenamed(OpenSignalConstants.pickup_zone_colName,OpenSignalConstants.zone_colName)
    log.info("Creating csv file for Insight2 data")
    saveData(insight2,SaveMode.Overwrite,OpenSignalConstants.analytics_type_two,landingDirectory)

    val insight3 = filterNullData.groupBy(OpenSignalConstants.pickup_borough_colName)
      .agg(
        count(OpenSignalConstants.pickup_borough_colName)
          .as(OpenSignalConstants.number_of_pickups_alias)
      )
      .withColumnRenamed(OpenSignalConstants.pickup_borough_colName,OpenSignalConstants.borough_colName)
    log.info("Creating csv file for Insight3 data")
    saveData(insight3,SaveMode.Overwrite,OpenSignalConstants.analytics_type_three,landingDirectory)

    // Insight 4 + 5
    val insight4 = filterNullData.groupBy(OpenSignalConstants.dropoff_zone_colName)
      .agg(
        count(OpenSignalConstants.dropoff_zone_colName).as(OpenSignalConstants.number_of_dropoffs_alias)
        ,round(
          avg(OpenSignalConstants.trip_distance_colName),2
        ).as(OpenSignalConstants.average_trip_distance_alias)
        ,round(
          avg(OpenSignalConstants.total_amount_colName),2
        ).as(OpenSignalConstants.average_total_fare_alias)
      )
      .withColumnRenamed(OpenSignalConstants.dropoff_zone_colName,OpenSignalConstants.zone_colName)
    log.info("Creating csv file for Insight4 data")
    saveData(insight4,SaveMode.Overwrite,OpenSignalConstants.analytics_type_four,landingDirectory)

    val insight5 = filterNullData.groupBy(OpenSignalConstants.dropoff_borough_colName)
      .agg(
        count(OpenSignalConstants.dropoff_borough_colName).as(OpenSignalConstants.number_of_dropoffs_alias)
        ,round(
          avg(OpenSignalConstants.trip_distance_colName),2
        ).as(OpenSignalConstants.average_trip_distance_alias)
        ,round(
          avg(OpenSignalConstants.total_amount_colName),2
        ).as(OpenSignalConstants.average_total_fare_alias)
      )
      .withColumnRenamed("dropoff_zone","zone")
    log.info("Creating csv file for Insight5 data")
    saveData(insight5,SaveMode.Overwrite,OpenSignalConstants.analytics_type_five,landingDirectory)




    // Insight 6
    val insight6Temp = filterNullData.groupBy(OpenSignalConstants.pickup_zone_colName,OpenSignalConstants.dropoff_zone_colName)
      .agg(
        count(OpenSignalConstants.dropoff_zone_colName)
          .as(OpenSignalConstants.number_of_dropoffs_alias)
      )
    val window = Window.partitionBy(OpenSignalConstants.pickup_zone_colName)
      .orderBy(
        col(OpenSignalConstants.number_of_dropoffs_colName).desc
      )
    val insight6 = insight6Temp.withColumn(OpenSignalConstants.rank_colName,rank().over(window)).filter(col(OpenSignalConstants.rank_colName) <= 5)
    log.info("Creating csv file for Insight6 data")
    saveData(insight6,SaveMode.Overwrite,OpenSignalConstants.analytics_type_six,landingDirectory)


    // Insight 7
    val dataWithPickupDateHour = filterNullData.withColumn(
      OpenSignalConstants.pickup_date_colName,to_date(col(OpenSignalConstants.tpep_pickup_datetime_colName),OpenSignalConstants.dateFormat)
    ).withColumn(
      OpenSignalConstants.pickup_date_hour_colName,hour(col(OpenSignalConstants.tpep_pickup_datetime_colName))
    )
    val insight7Temp = dataWithPickupDateHour.groupBy(OpenSignalConstants.pickup_date_colName,OpenSignalConstants.pickup_date_hour_colName)
      .agg(
        count(col(OpenSignalConstants.pickup_date_hour_colName))
          .as(OpenSignalConstants.no_of_trips_per_hour_alias)
      )
      .withColumnRenamed(OpenSignalConstants.pickup_date_hour_colName,OpenSignalConstants.hour_of_day_colName)
    val insight7 = insight7Temp.groupBy(
      OpenSignalConstants.hour_of_day_colName
    ).agg(
      avg(OpenSignalConstants.no_of_trips_per_hour_colName).as(OpenSignalConstants.average_trips_alias)
    ).orderBy($"hour_of_day")
    log.info("Creating csv file for Insight7 data")
    saveData(insight7,SaveMode.Overwrite,OpenSignalConstants.analytics_type_seven,landingDirectory)



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
