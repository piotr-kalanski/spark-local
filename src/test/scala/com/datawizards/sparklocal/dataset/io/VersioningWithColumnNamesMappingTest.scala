package com.datawizards.sparklocal.dataset.io

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.TestModel.{PersonWithMapping, PersonWithMappingV2, PersonWithMappingV3}
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.datastore._
import com.datawizards.sparklocal.impl.scala.eager.dataset.io.ReaderScalaEagerImpl
import com.datawizards.sparklocal.impl.spark.dataset.io.ReaderSparkImpl
import com.datawizards.sparklocal.implicits._
import org.apache.spark.sql.SaveMode
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class VersioningWithColumnNamesMappingTest extends SparkLocalBaseTest {

  private val connectionString = "jdbc:h2:mem:test"

  val people = Seq(
    PersonWithMapping("p1", 10),
    PersonWithMapping("p2", 20),
    PersonWithMapping("p3", 30),
    PersonWithMapping("p4", 40)
  )

  val peopleV2 = Seq(
    PersonWithMappingV2("p1", 10, Some("Mr")),
    PersonWithMappingV2("p2", 20, Some("Ms")),
    PersonWithMappingV2("p3", 30, None),
    PersonWithMappingV2("p4", 40, Some("Mr"))
  )

  test("Versioning - CSV - Spark") {
    versioningTestScenarioSpark(CSVDataStore("target/people_mapping_v2_spark.csv"))
  }

  test("Versioning - CSV - Scala") {
    versioningTestScenarioScala(CSVDataStore("target/people_mapping_v2_scala.csv"))
  }

  test("Versioning - JSON - Spark") {
    versioningTestScenarioSpark(JsonDataStore("target/people_mapping_v2_spark.json"))
  }

  test("Versioning - JSON - Scala") {
    versioningTestScenarioScala(JsonDataStore("target/people_mapping_v2_scala.json"))
  }

  test("Versioning - parquet - Spark") {
    versioningTestScenarioSpark(ParquetDataStore("target/people_mapping_v2_spark.parquet"))
  }

  test("Versioning - parquet - Scala") {
    versioningTestScenarioScala(ParquetDataStore("target/people_mapping_v2_scala.parquet"))
  }

  test("Versioning - avro - Spark") {
    versioningTestScenarioSpark(AvroDataStore("target/people_mapping_v2_spark.avro"))
  }

  test("Versioning - avro - Scala") {
    versioningTestScenarioScala(AvroDataStore("target/people_mapping_v2_scala.avro"))
  }

  test("Versioning - hive - Spark") {
    versioningTestScenarioSpark(HiveDataStore("default", "people_mapping_v2_spark"))
  }

  test("Versioning - hive - Scala") {
    versioningTestScenarioScala(HiveDataStore("default", "people_mapping_v2_scala"))
  }

  test("Versioning - JDBC - Spark") {
    versioningTestScenarioSpark(H2DataStore(connectionString, "public", "PEOPLE_MAPPING_V2_SPARK", new java.util.Properties()))
  }

  test("Versioning - JDBC - Scala") {
    versioningTestScenarioScala(H2DataStore(connectionString, "public", "PEOPLE_MAPPING_V2_SCALA", new java.util.Properties()))
  }

  private def versioningTestScenarioScala(dataStore: DataStore): Unit = {
    versioningTestScenario(DataSetAPI(peopleV2), dataStore, ReaderScalaEagerImpl)
  }

  private def versioningTestScenarioSpark(dataStore: DataStore): Unit = {
    import spark.implicits._
    versioningTestScenario(DataSetAPI(peopleV2.toDS()), dataStore, ReaderSparkImpl)
  }

  private def versioningTestScenario(ds: DataSetAPI[PersonWithMappingV2], dataStore: DataStore, reader: Reader): Unit = {
    val expectedPerson = people.toArray
    val expectedPersonV3 = peopleV2.map(p => PersonWithMappingV3(p.name, p.age, p.title, None)).toArray
    ds.write(dataStore, SaveMode.Overwrite)
    // Read previous version
    assertDatasetOperationResultWithSorted(reader.read[PersonWithMapping](dataStore)) {
      expectedPerson
    }
    // Read future version
    assertDatasetOperationResultWithSorted(reader.read[PersonWithMappingV3](dataStore)) {
      expectedPersonV3
    }
  }

}
