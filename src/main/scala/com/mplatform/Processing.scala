package com.mplatform

import org.apache.spark.sql._

object Processing extends {
  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName("ANN export")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    val schema = Encoders.product[Dto].schema

    val persistedRdd: DataFrame = spark.read
      .option("header", "true")
      .schema(schema)
      .json("/Users/sergey.gulido/Documents/adobe/profiles/export_20191009_1251_23271")
      .filter("visitorMappings is not null")
      .filter("dspId == 1")

    val ds = persistedRdd.as[Dto]
    import org.apache.spark.sql.functions.collect_list

    val resultDs: Dataset[AdobeFormat] = ds.flatMap(_.toMapping)
      .groupBy("visitorId", "deviceType", "accId", "seatId")
      .agg(collect_list("addReduced") as "adds", collect_list("delReduced") as "dels").as[GroupedMapping]
      .map(_.toAdobeFormat)

    resultDs.coalesce(8)
      .write
      .partitionBy("accId", "seatId", "deviceType")
      .mode(SaveMode.Overwrite)
      .option("compression", "gzip")
      .text("/Users/sergey.gulido/Documents/adobe/out")

  }

}


