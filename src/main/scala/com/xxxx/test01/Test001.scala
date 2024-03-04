package com.xxxx.test01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object Test001 {

  case class Schema(peer_id: String, id_1: String, id_2: String, year: Integer)

  def parseSchema(str: String): Schema = {
    val fields = str.split("\\s+")
    assert(fields.size == 4)
    Schema(fields(0), fields(1), fields(2), fields(3).toInt)
  }

  def main(args: Array[String]): Unit = {
    //build connection
    val conf = new SparkConf().setMaster("local[*]").setAppName("test001")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //Log Level
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("ERROR")

    import spark.implicits._

    //read from text file
    val rdd: RDD[Schema] = sc.textFile("data/test001.txt").map(parseSchema)
    val df: DataFrame = rdd.toDF()
    df.show()

    val givenNum = 3

    val res: DataFrame = Demo(df, givenNum)
    res.show()

    //close connection
    spark.stop()
  }

  private def Demo(df: DataFrame, givenNum: Int): DataFrame = {

    val dfAft01: Dataset[Row] = df.filter(df.col("peer_id").contains(df.col("id_2")))
      .selectExpr("peer_id", "year as year1")
    dfAft01.show()

    val dfAft02: DataFrame = df.groupBy("peer_id", "year").count()
      .join(dfAft01, Seq("peer_id"), "left")
      .filter(df.col("year") <= dfAft01.col("year1"))
      .select("peer_id", "year", "count")
    dfAft02.show()

    //define a window
    val windowSpec: WindowSpec = Window.partitionBy("peer_id").orderBy(df.col("year").desc)
    val dfBef03: DataFrame = dfAft02.withColumn("cumul_amount", sum("count").over(windowSpec))
    val dfAft03: DataFrame = dfBef03.filter((dfBef03.col("cumul_amount") - dfBef03.col("count")) < givenNum)
      .select("peer_id", "year")
    //dfAft03.show()

    return dfAft03
  }
}
