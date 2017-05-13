package com.datawizards.sparklocal.examples.dataset

import com.datawizards.sparklocal.dataset.io.{Reader, ReaderScalaImpl, ReaderSparkImpl}
import com.datawizards.sparklocal.datastore.CSVDataStore
import com.datawizards.sparklocal.examples.dataset.Model.Person
import org.apache.spark.sql.SparkSession

object ExampleCSV extends App {
  val spark = SparkSession.builder().master("local").getOrCreate()

  val file = getClass.getResource("/people.csv").getPath
  assert(countByGender(ReaderScalaImpl, file) == countByGender(ReaderSparkImpl, file))

  def countByGender(reader: Reader, path: String): Map[String, Long] = {
    val ds = reader.read(CSVDataStore[Person](path))
    ds
      .groupByKey(_.gender)
      .count()
      .collect()
      .toMap
  }
}
