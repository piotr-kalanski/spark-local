package com.datawizards.sparklocal.dataset.io

import java.io.File
import java.nio.file.{Files, Paths}
import java.sql.DriverManager

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.TestModel.{Person, PersonUppercase, PersonV2, PersonV3, PersonWithMapping}
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.datastore._
import com.datawizards.sparklocal.impl.scala.eager.dataset.io.ReaderScalaEagerImpl
import com.datawizards.sparklocal.impl.spark.dataset.io.ReaderSparkImpl
import com.datawizards.sparklocal.implicits._
import org.apache.spark.sql.SaveMode
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ColumnNamesFromMappingTest extends SparkLocalBaseTest {

  val peopleWithMapping = Seq(
    PersonWithMapping("p1", 10),
    PersonWithMapping("p2", 20),
    PersonWithMapping("p3", 30),
    PersonWithMapping("p4", 40)
  )

  val people = Seq(
    Person("p1", 10),
    Person("p2", 20),
    Person("p3", 30),
    Person("p4", 40)
  )

  val peopleUppercase = Seq(
    PersonUppercase("p1", 10),
    PersonUppercase("p2", 20),
    PersonUppercase("p3", 30),
    PersonUppercase("p4", 40)
  )

  private val connectionString = "jdbc:h2:mem:test"

  lazy val peopleWithMappingDataSetScala = DataSetAPI(peopleWithMapping)
  lazy val peopleWithMappingDataSetSpark = DataSetAPI(peopleWithMapping.toDS())
  lazy val peopleDataSetScala = DataSetAPI(people)
  lazy val peopleDataSetSpark = DataSetAPI(people.toDS())
  lazy val peopleUppercaseDataSetScala = DataSetAPI(peopleUppercase)
  lazy val peopleUppercaseDataSetSpark = DataSetAPI(peopleUppercase.toDS())

  private def readFileContentFromDirectory(directory: String, postfix: String): Array[Byte] = {
    val dir = new File(directory)
    val file = dir.listFiles().filter(f => f.getName.endsWith(postfix)).head
    readFileContent(file.getPath)
  }

  private def readFileContent(file: String): Array[Byte] =
    Files.readAllBytes(Paths.get(file))

  private def columnMappingTestScenario(
                                         dsRaw: DataSetAPI[Person],
                                         dsWithMapping: DataSetAPI[PersonWithMapping],
                                         dataStoreRaw: DataStore,
                                         dataStoreWithMapping: DataStore,
                                         pathRaw: String,
                                         pathWithMapping: String,
                                         reader: Reader,
                                         postfix: String
                                       ): Unit = {
    dsWithMapping.write(dataStoreWithMapping, SaveMode.Overwrite)
    dsRaw.write(dataStoreRaw, SaveMode.Overwrite)
    assert(readFileContentFromDirectory(pathWithMapping, postfix) != readFileContentFromDirectory(pathRaw, postfix))
    assertDatasetOperationResultWithSorted(reader.read[PersonWithMapping](dataStoreWithMapping)) {
      peopleWithMapping.toArray
    }
  }

  private def columnMappingTestScenarioWithFileDataStore(
                                                          dsRaw: DataSetAPI[Person],
                                                          dsWithMapping: DataSetAPI[PersonWithMapping],
                                                          dataStoreRaw: FileDataStore,
                                                          dataStoreWithMapping: FileDataStore,
                                                          reader: Reader
                                                        ): Unit =
    columnMappingTestScenario(
      dsRaw,
      dsWithMapping,
      dataStoreRaw,
      dataStoreWithMapping,
      dataStoreRaw.path,
      dataStoreWithMapping.path,
      reader,
      dataStoreRaw.extension
    )

  private def columnMappingTestScenarioWithFileDataStoreSpark(
                                                               dataStoreRaw: FileDataStore,
                                                               dataStoreWithMapping: FileDataStore
                                                             ): Unit = {
    spark  // to initialize spark session
    columnMappingTestScenarioWithFileDataStore(
      peopleDataSetSpark,
      peopleWithMappingDataSetSpark,
      dataStoreRaw,
      dataStoreWithMapping,
      ReaderSparkImpl
    )
  }

  private def columnMappingTestScenarioWithFileDataStoreScala(
                                                               dataStoreRaw: FileDataStore,
                                                               dataStoreWithMapping: FileDataStore
                                                             ): Unit = {
    columnMappingTestScenarioWithFileDataStore(
      peopleDataSetScala,
      peopleWithMappingDataSetScala,
      dataStoreRaw,
      dataStoreWithMapping,
      ReaderScalaEagerImpl
    )
  }

  private def columnMappingTestScenarioSpark(
                                              dataStoreRaw: DataStore,
                                              dataStoreWithMapping: DataStore,
                                              pathRaw: String,
                                              pathWithMapping: String,
                                              postfix: String
                                            ): Unit = {
    spark  // to initialize spark session
    columnMappingTestScenario(
      peopleDataSetSpark,
      peopleWithMappingDataSetSpark,
      dataStoreRaw,
      dataStoreWithMapping,
      pathRaw,
      pathWithMapping,
      ReaderSparkImpl,
      postfix
    )
  }

  private def columnMappingTestScenarioScala(
                                              dataStoreRaw: DataStore,
                                              dataStoreWithMapping: DataStore,
                                              pathRaw: String,
                                              pathWithMapping: String,
                                              postfix: String
                                            ): Unit = {
    columnMappingTestScenario(
      peopleDataSetScala,
      peopleWithMappingDataSetScala,
      dataStoreRaw,
      dataStoreWithMapping,
      pathRaw,
      pathWithMapping,
      ReaderScalaEagerImpl,
      postfix
    )
  }

  private def columnMappingJdbcTestScenarioSpark(dataStoreRaw: JdbcDataStore,
                                                 dataStoreWithMapping: JdbcDataStore): Unit = {
    spark  // to initialize spark session
    columnMappingJdbcTestScenario(
      peopleUppercaseDataSetSpark,
      peopleWithMappingDataSetSpark,
      dataStoreRaw,
      dataStoreWithMapping,
      ReaderSparkImpl
    )
  }

  private def columnMappingJdbcTestScenarioScala(dataStoreRaw: JdbcDataStore,
                                                 dataStoreWithMapping: JdbcDataStore): Unit = {
    columnMappingJdbcTestScenario(
      peopleUppercaseDataSetScala,
      peopleWithMappingDataSetScala,
      dataStoreRaw,
      dataStoreWithMapping,
      ReaderScalaEagerImpl
    )
  }

  private def columnMappingJdbcTestScenario(dsRaw: DataSetAPI[PersonUppercase],
                                                 dsWithMapping: DataSetAPI[PersonWithMapping],
                                                 dataStoreRaw: JdbcDataStore,
                                                 dataStoreWithMapping: JdbcDataStore,
                                                 reader: Reader): Unit = {
    dsWithMapping.write(dataStoreWithMapping, SaveMode.Overwrite)
    dsRaw.write(dataStoreRaw, SaveMode.Overwrite)
    assertDatasetOperationResultWithSorted(reader.read[PersonWithMapping](dataStoreWithMapping)) {
      peopleWithMapping.toArray
    }

    val connection = DriverManager.getConnection(connectionString, "", "")
    val rawResult = connection.createStatement().executeQuery(s"select * from ${dataStoreRaw.fullTableName}")
    val personMappingResult = connection.createStatement().executeQuery(s"select * from ${dataStoreWithMapping.fullTableName}")
    assert(rawResult.getMetaData.getColumnName(1) != personMappingResult.getMetaData.getColumnName(1))
    assert(rawResult.getMetaData.getColumnName(2) != personMappingResult.getMetaData.getColumnName(2))
    connection.close()
  }

  test("Column mapping - CSV - Spark") {
    columnMappingTestScenarioWithFileDataStoreSpark(
      CSVDataStore("target/people_raw_spark.csv"),
      CSVDataStore("target/people_mapping_spark.csv")
    )
  }

  test("Column mapping - json - Spark") {
    columnMappingTestScenarioWithFileDataStoreSpark(
      JsonDataStore("target/people_raw_spark.json"),
      JsonDataStore("target/people_mapping_spark.json")
    )
  }

  test("Column mapping - parquet - Spark") {
    columnMappingTestScenarioWithFileDataStoreSpark(
      ParquetDataStore("target/people_raw_spark.parquet"),
      ParquetDataStore("target/people_mapping_spark.parquet")
    )
  }

  test("Column mapping - avro - Spark") {
    columnMappingTestScenarioWithFileDataStoreSpark(
      AvroDataStore("target/people_raw_spark.avro"),
      AvroDataStore("target/people_mapping_spark.avro")
    )
  }

  test("Column mapping - hive - Spark") {
    columnMappingTestScenarioSpark(
      HiveDataStore("default", "people_raw_spark"),
      HiveDataStore("default", "people_mapping_spark"),
      "spark-warehouse/people_raw_spark",
      "spark-warehouse/people_mapping_spark",
      "parquet"
    )
  }

  test("Column mapping - JDBC - Spark") {
    columnMappingJdbcTestScenarioSpark(
      H2DataStore(connectionString, "public", "PEOPLE_RAW_SPARK", new java.util.Properties()),
      H2DataStore(connectionString, "public", "PEOPLE_MAPPING_SPARK", new java.util.Properties())
    )
  }

  test("Column mapping - CSV - Scala") {
    columnMappingTestScenarioWithFileDataStoreScala(
      CSVDataStore("target/people_raw_scala.csv"),
      CSVDataStore("target/people_mapping_scala.csv")
    )
  }

  test("Column mapping - json - Scala") {
    columnMappingTestScenarioWithFileDataStoreScala(
      JsonDataStore("target/people_raw_scala.json"),
      JsonDataStore("target/people_mapping_scala.json")
    )
  }

  test("Column mapping - parquet - Scala") {
    columnMappingTestScenarioWithFileDataStoreScala(
      ParquetDataStore("target/people_raw_scala.parquet"),
      ParquetDataStore("target/people_mapping_scala.parquet")
    )
  }

  test("Column mapping - avro - Scala") {
    columnMappingTestScenarioWithFileDataStoreScala(
      AvroDataStore("target/people_raw_scala.avro"),
      AvroDataStore("target/people_mapping_scala.avro")
    )
  }

  test("Column mapping - hive - Scala") {
    columnMappingTestScenarioScala(
      HiveDataStore("default", "people_raw_scala"),
      HiveDataStore("default", "people_mapping_scala"),
      "spark-warehouse/default/people_raw_scala",
      "spark-warehouse/default/people_mapping_scala",
      "avro"
    )
  }

  test("Column mapping - JDBC - Scala") {
    columnMappingJdbcTestScenarioScala(
      H2DataStore(connectionString, "public", "PEOPLE_RAW_SCALA", new java.util.Properties()),
      H2DataStore(connectionString, "public", "PEOPLE_MAPPING_SCALA", new java.util.Properties())
    )
  }

  // TODO - add tests for versioning + column mapping


}
