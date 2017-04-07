package com.epam.training.spark.core

import com.epam.training.spark.core.domain.Climate
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Homework {
  val DELIMITER = ";"
  val RAW_BUDAPEST_DATA = "data/budapest_daily_1901-2010.csv"
  val OUTPUT_DUR = "output"

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("EPAM BigData training Spark Core homework")
      .setIfMissing("spark.master", "local[2]")
      .setIfMissing("spark.sql.shuffle.partitions", "10")
    val sc = new SparkContext(sparkConf)

    processData(sc)

    sc.stop()

  }

  def processData(sc: SparkContext): Unit = {

    /**
      * Task 1
      * Read raw data from provided file, remove header, split rows by delimiter
      */
    val rawData: RDD[List[String]] = getRawDataWithoutHeader(sc, Homework.RAW_BUDAPEST_DATA)

    /**
      * Task 2
      * Find errors or missing values in the data
      */
    val errors: List[Int] = findErrors(rawData)
    println(errors)

    /**
      * Task 3
      * Map raw data to Climate type
      */
    val climateRdd: RDD[Climate] = mapToClimate(rawData)

    /**
      * Task 4
      * List average temperature for a given day in every year
      */
    val averageTemeperatureRdd: RDD[Double] = averageTemperature(climateRdd, 1, 2)

    /**
      * Task 5
      * Predict temperature based on mean temperature for every year including 1 day before and after
      * For the given month 1 and day 2 (2nd January) include days 1st January and 3rd January in the calculation
      */
    val predictedTemperature: Double = predictTemperature(climateRdd, 1, 2)
    println(s"Predicted temperature: $predictedTemperature")

  }

  def getRawDataWithoutHeader(sc: SparkContext, rawDataPath: String): RDD[List[String]] = {
    val rddBudapest = sc.textFile(rawDataPath).cache()
    rddBudapest
      .filter(!_.startsWith("#"))
      .map(_.split(Homework.DELIMITER, 7))
      .map(_.toList)
  }

  def findErrors(rawData: RDD[List[String]]): List[Int] = {
    rawData
      .map(list => list.map(f => if (f == null || f.isEmpty) {1} else {0}))
      .reduce((list1, list2) => List(list1(0) + list2(0), list1(1) + list2(1), list1(2) + list2(2),
        list1(3) + list2(3), list1(4) + list2(4), list1(5) + list2(5), list1(6) + list2(6)))
      .toList
  }

  def mapToClimate(rawData: RDD[List[String]]): RDD[Climate] = {
    rawData
      .map(x => Climate.apply(x(0), x(1), x(2), x(3), x(4), x(5), x(6)))
  }

  def averageTemperature(climateData: RDD[Climate], month: Int, dayOfMonth: Int): RDD[Double] = {
    climateData
      .filter(x => (x.observationDate.getMonth.getValue == month) && (x.observationDate.getDayOfMonth == dayOfMonth))
      .map{x => (x.observationDate, x.meanTemperature)}
      .sortBy(_._2.value, true)
      .map(_._2.value)
  }

  def predictTemperature(climateData: RDD[Climate], month: Int, dayOfMonth: Int): Double = {
    climateData
      .filter(x => ((x.observationDate.getMonth.getValue == month) && (x.observationDate.getDayOfMonth == dayOfMonth))
        || ((x.observationDate.minusDays(1).getMonth.getValue == month) && (x.observationDate.minusDays(1).getDayOfMonth == dayOfMonth))
        || ((x.observationDate.plusDays(1).getMonth.getValue == month) && (x.observationDate.plusDays(1).getDayOfMonth == dayOfMonth))
      )
      .map(x =>  ((month, dayOfMonth),x.meanTemperature) )
      .aggregateByKey((0.0, 0.0))(
        (acc, value) => (acc._1 + value.value, acc._2 + 1),
        (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
      .mapValues(sumCount => sumCount._1 / sumCount._2)
      .map(x=> x._2)
      .first()
  }


}


