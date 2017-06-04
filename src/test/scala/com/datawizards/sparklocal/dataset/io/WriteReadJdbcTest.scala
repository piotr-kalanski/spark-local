package com.datawizards.sparklocal.dataset.io

import java.sql.DriverManager

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.TestModel.Person
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.datastore.JdbcDataStore
import com.datawizards.sparklocal.impl.scala.eager.dataset.io.ReaderScalaImpl
import com.datawizards.sparklocal.impl.spark.dataset.io.ReaderSparkImpl
import com.datawizards.sparklocal.implicits._
import org.apache.spark.sql.SaveMode
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class WriteReadJdbcTest extends SparkLocalBaseTest {

  val connectionString = "jdbc:h2:mem:test"
  val data = Seq(
    Person("p1", 10),
    Person("p2", 20),
    Person("p3", 30),
    Person("p4", 40)
  )

  test("Writing and reading JDBC table produces the same result - Scala") {
    val dataStore = JdbcDataStore(connectionString, "public", "people_scala", new java.util.Properties(), "org.h2.Driver")
    val connection = DriverManager.getConnection(connectionString, "", "")
    connection.createStatement().execute("create table people_scala(name VARCHAR, age INT)")
    val expected = DataSetAPI(data)
    expected.write(dataStore, SaveMode.Append)
    assertResult(expected) {
      ReaderScalaImpl.read[Person](dataStore)
    }
    connection.close()
  }

  test("Writing and reading JDBC table produces the same result - Spark") {
    val dataStore = JdbcDataStore(connectionString, "public", "people_spark", new java.util.Properties(), "org.h2.Driver")
    val connection = DriverManager.getConnection(connectionString, "", "")
    connection.createStatement().execute("""create table people_spark("name" VARCHAR, "age" INT)""")
    spark // just to initialize SparkSession
    val expected = DataSetAPI(data.toDS())
    expected.write(dataStore, SaveMode.Append)
    assertResult(expected) {
      ReaderSparkImpl.read[Person](dataStore)
    }
    connection.close()
  }

}