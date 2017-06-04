package com.datawizards.sparklocal.examples.dataset

import com.datawizards.sparklocal.dataset.io.Reader
import com.datawizards.sparklocal.datastore.CSVDataStore
import com.datawizards.sparklocal.examples.dataset.Model.Person
import com.datawizards.sparklocal.impl.scala.eager.dataset.io.ReaderScalaEagerImpl
import com.datawizards.sparklocal.impl.spark.dataset.io.ReaderSparkImpl
import com.datawizards.sparklocal.implicits._
import org.apache.spark.sql.SparkSession

object ExampleCSV extends App {
  val spark = SparkSession.builder().master("local").getOrCreate()

  val file = getClass.getResource("/people.csv").getPath
  assert(countByGender(ReaderScalaEagerImpl, file) == countByGender(ReaderSparkImpl, file))

  def countByGender(reader: Reader, path: String): Map[String, Long] = {
    val ds = reader.read[Person](CSVDataStore(path))
    ds
      .groupByKey(_.gender)
      .count()
      .collect()
      .toMap
  }
}
