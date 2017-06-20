package com.datawizards.sparklocal.dataset.io

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.TestModel.{Person, PersonV2, PersonV3}
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.datastore._
import com.datawizards.sparklocal.impl.scala.eager.dataset.io.ReaderScalaEagerImpl
import com.datawizards.sparklocal.impl.spark.dataset.io.ReaderSparkImpl
import com.datawizards.sparklocal.implicits._
import org.apache.spark.sql.SaveMode
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class VersioningTest extends SparkLocalBaseTest {

  val people = Seq(
    Person("p1", 10),
    Person("p2", 20),
    Person("p3", 30),
    Person("p4", 40)
  )

  val peopleV2 = Seq(
    PersonV2("p1", 10, Some("Mr")),
    PersonV2("p2", 20, Some("Ms")),
    PersonV2("p3", 30, None),
    PersonV2("p4", 40, Some("Mr"))
  )

  test("Versioning - CSV - Spark") {
    versioningTestScenarioSpark(CSVDataStore("target/people_v2_spark.csv"))
  }

  test("Versioning - CSV - Scala") {
    versioningTestScenarioScala(CSVDataStore("target/people_v2_scala.csv"))
  }

  test("Versioning - JSON - Spark") {
    versioningTestScenarioSpark(JsonDataStore("target/people_v2_spark.json"))
  }

  test("Versioning - JSON - Scala") {
    versioningTestScenarioScala(JsonDataStore("target/people_v2_scala.json"))
  }

  test("Versioning - parquet - Spark") {
    versioningTestScenarioSpark(ParquetDataStore("target/people_v2_spark.parquet"))
  }

  test("Versioning - parquet - Scala") {
    versioningTestScenarioScala(ParquetDataStore("target/people_v2_scala.parquet"))
  }

  test("Versioning - avro - Spark") {
    versioningTestScenarioSpark(AvroDataStore("target/people_v2_spark.avro"))
  }

  test("Versioning - avro - Scala") {
    versioningTestScenarioScala(AvroDataStore("target/people_v2_scala.avro"))
  }

  test("Versioning - hive - Spark") {
    versioningTestScenarioSpark(HiveDataStore("default", "people_v2_spark"))
  }

  test("Versioning - hive - Scala") {
    versioningTestScenarioScala(HiveDataStore("default", "people_v2_scala"))
  }

  private def versioningTestScenarioScala(dataStore: DataStore): Unit = {
    versioningTestScenario(DataSetAPI(peopleV2), dataStore, ReaderScalaEagerImpl)
  }

  private def versioningTestScenarioSpark(dataStore: DataStore): Unit = {
    versioningTestScenario(DataSetAPI(peopleV2.toDS()), dataStore, ReaderSparkImpl)
  }

  private def versioningTestScenario(ds: DataSetAPI[PersonV2], dataStore: DataStore, reader: Reader): Unit = {
    val expectedPerson = people.toArray
    val expectedPersonV3 = peopleV2.map(p => PersonV3(p.name, p.age, p.title, None)).toArray
    ds.write(dataStore, SaveMode.Overwrite)
    // Read previous version
    assertDatasetOperationResultWithSorted(reader.read[Person](dataStore)) {
      expectedPerson
    }
    // Read future version
    assertDatasetOperationResultWithSorted(reader.read[PersonV3](dataStore)) {
      expectedPersonV3
    }
  }

}
