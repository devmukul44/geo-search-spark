package com.whiletruecurious.geosearch

import com.whiletruecurious.geosearch.models.Coordinate
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

object Search {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("com.whiletruecurious.geosearch-batch-job")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    // Setting log level to error
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)

    // file path
    val masterDataPath = "src/main/resources/data-2.csv"
    val searchDataPath = "src/main/resources/data-1.csv"
    val outputPath = "src/main/resources/output/geolocation"

    // Use of scala curring and partial functions
    val getCsvPartialDF = getCsv(sqlContext)_

    // Reading DataFrames for OriginalDataSet and ContinuousDataSet
    val masterDataFrame = getCsvPartialDF(masterDataPath)
    val searchDataFrame = getCsvPartialDF(searchDataPath)

    // Limits
    val latLimit = 0.5
    val lonLimit = 4.0
    val maxDist = 50.0

    // Broadcasting Master Data
    val broadcastMasterDF = broadcastMasterDataFrame(sqlContext, masterDataFrame)

    val solDF = search(sqlContext, broadcastMasterDF, searchDataFrame, latLimit, lonLimit, maxDist)

    // saving to output path
    solDF.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .option("header","true")
      .save(outputPath)
  }

  /** The core search logic implementation. This uses a broadcasted sorted List of Coordinate objects as an index.
    * The logic implemented in this method first goes through the index data and filters out nearest matching
    * Coordinate (Id, latitude and longitude value) objects.
    * Now it applies linear search to find the available coordinate with the smallest distance which also should be
    * less than the max search radius specified.
    *
    * @param sqlContext       SQLContext
    * @param broadcastIndex   Available latitude longitude Spark Broadcast variable
    * @param searchDataFrame  A spark DataFrame of the Coordinates that are to be mapped.
    * @return
    */
  def search(sqlContext: SQLContext, broadcastIndex: Broadcast[List[Coordinate]], searchDataFrame: DataFrame, latLimit: Double, lonLimit: Double, maxDist: Double): DataFrame = {
    val indexList = broadcastIndex.value

    // For each Coordinate in DataFrame the lookup should find one coordinate from the index of available coordinates
    // Mapper function which maps one search Coordinate to one of the available or returns a null
    val mapper = (searchId: String, searchLat: Double, searchLon: Double) => {

      // Filtering out the index coordinates
      // TODO: This Filter logic can be implemented using binary search to reduce complexity.
      val filterIndexList = indexList.filter{indexCoordinate =>
        val indexLat = indexCoordinate.lat
        val indexLon = indexCoordinate.lon
        // Filter Predicate
        (searchLat - latLimit < indexLat || searchLat + latLimit > indexLat) &&
          (searchLon - lonLimit < indexLon || searchLon + lonLimit < indexLon)
      }

      // Calculate distance for all indices left and take the min one
      filterIndexList match {
        case a if filterIndexList.nonEmpty => // Some values exists for which the distance has to be calculated
          val search = Coordinate(searchId, searchLat, searchLon)
          val nearestCoordinateDistTuple = filterIndexList
            .map(c => (c, geoDist(c, search))) // Calculate distance for all points
            .reduce((a, b) => if (a._2 < b._2) a else b) // Find min dist
          if (nearestCoordinateDistTuple._2 < maxDist) // Check if it stills falls within the max dist limit
            Some(nearestCoordinateDistTuple._1)
          else
            None
        case _ => // No match found
          None
      }
    }

    val mapper_udf = udf(mapper)

    searchDataFrame.withColumn("_mappedCoordinate", mapper_udf(col("id2"), col("lat"), col("long")))
      .select(col("id2"), col("_mappedCoordinate.id").as("id1"))
  }


  /** Read the DataFrame of available id, latitudes and longitudes and broadcast them.
    *
    * @param sqlContext
    * @param df
    * @return
    */
  def broadcastMasterDataFrame(sqlContext: SQLContext, df: DataFrame): Broadcast[scala.List[Coordinate]] = {
    val castDF = df
      .withColumn("lat", col("lat").cast(DoubleType))
      .withColumn("long", col("long").cast(DoubleType))
      .coalesce(1) // distributed sorting is not used since the index would be fairly small in size
      .sort(asc("lat"), asc("long"))
      .map(toCoordinate)
    sqlContext.sparkContext.broadcast(castDF.collect().toList)
  }

  /** Haversine Geo Distance formula to calculate geo-distance between two
    * points.
    *
    * @param ilat1
    * @param ilon1
    * @param ilat2
    * @param ilon2
    * @return Distance between point (ilat1,ilon1) and (ilat2,ilon2)
    */
  def geoDist(ilat1: Double, ilon1: Double, ilat2: Double, ilon2: Double): Double = {
    val long2 = ilon2 * math.Pi / 180
    val lat2 = ilat2 * math.Pi / 180
    val long1 = ilon1 * math.Pi / 180
    val lat1 = ilat1 * math.Pi / 180

    val dlon = long2 - long1
    val dlat = lat2 - lat1
    val a = math.pow(math.sin(dlat / 2), 2) + math.cos(lat1) * math.cos(lat2) * math.pow(math.sin(dlon / 2), 2)
    val c = 2 * math.atan2(Math.sqrt(a), math.sqrt(1 - a))
    val result = 6373 * c
    // for Miles, use 3961 instead
    result
  }

  /** Distance between two Coordinate Objects
    *
    * @param c1
    * @param c2
    * @return
    */
  def geoDist(c1: Coordinate, c2: Coordinate): Double = {
    geoDist(c1.lat, c1.lon, c2.lat, c2.lon)
  }

  /** Returns DataFrame for csv file present in specified `filepath`
    *
    * Since Spark reader is used for reading, the index File can be present in HDFS / S3 or any distributed file system
    * supported by Spark.
    *
    * @param sqlContext
    * @param path
    * @return
    */
  def getCsv(sqlContext: SQLContext)(path: String): DataFrame = {
    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header","true")
      .load(path)
  }

  /** Utility method to convert a `Row` of (id, Latitude and Longitude) to a Coordinate Object
    *
    * @param row
    * @return
    */
  def toCoordinate(row: Row): Coordinate = Coordinate(row.getString(0), row.getDouble(1), row.getDouble(2))
}
