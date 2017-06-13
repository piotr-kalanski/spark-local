package com.datawizards.sparklocal.dataset.io

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.TestModel.Person
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.datastore.HiveDataStore
import com.datawizards.sparklocal.impl.scala.eager.dataset.io.ReaderScalaEagerImpl
import com.datawizards.sparklocal.impl.spark.dataset.io.ReaderSparkImpl
import com.datawizards.sparklocal.implicits._
import org.apache.spark.sql.SaveMode
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class WriteReadHiveTest extends SparkLocalBaseTest {

  val data = Seq(
    Person("p1", 10),
    Person("p2", 20),
    Person("p3", 30),
    Person("p4", 40)
  )

  test("Writing and reading table produces the same result - Scala") {
    val expected = DataSetAPI(data)
    val dataStore = HiveDataStore("default", "people_scala")
    expected.write(dataStore, SaveMode.Overwrite)
    assertResult(expected) {
      ReaderScalaEagerImpl.read[Person](dataStore)
    }
  }

  test("Writing and reading table produces the same result - Spark") {
    import spark.implicits._
    val expected = DataSetAPI(data.toDS())
    val dataStore = HiveDataStore("default", "people_spark")
    expected.write(dataStore, SaveMode.Overwrite)
    assertResult(expected) {
      ReaderSparkImpl.read[Person](dataStore)
    }
  }

  test("Trying to write to existing file should throw exception - Scala") {
    val expected = DataSetAPI(data)
    val dataStore = HiveDataStore("default", "people_scala_error")
    expected.write(dataStore, SaveMode.Overwrite)
    intercept[Exception] {
      expected.write(dataStore, SaveMode.ErrorIfExists)
    }
  }

  test("Trying to write to existing table should throw exception - Spark") {
    import spark.implicits._
    val expected = DataSetAPI(data.toDS())
    val dataStore = HiveDataStore("default", "people_spark_error")
    expected.write(dataStore, SaveMode.Overwrite)
    intercept[Exception] {
      expected.write(dataStore, SaveMode.ErrorIfExists)
    }
  }

  test("Writing twice to same table produces single result - Scala") {
    val expected = DataSetAPI(data)
    val dataStore = HiveDataStore("default", "people_scala_twice")
    expected.union(expected).write(dataStore, SaveMode.Overwrite)
    expected.write(dataStore, SaveMode.Overwrite)
    assertResult(expected) {
      ReaderScalaEagerImpl.read[Person](dataStore)
    }
  }

  test("Writing twice to same table produces single result - Spark") {
    import spark.implicits._
    val expected = DataSetAPI(data.toDS())
    val dataStore = HiveDataStore("default", "people_spark_twice")
    expected.union(expected).write(dataStore, SaveMode.Overwrite)
    expected.write(dataStore, SaveMode.Overwrite)
    assertResult(expected) {
      ReaderSparkImpl.read[Person](dataStore)
    }
  }

  test("Writing to existing table with ignore - Scala") {
    val expected = DataSetAPI(data)
    val dataStore = HiveDataStore("default", "people_scala_ignore")
    expected.write(dataStore, SaveMode.Overwrite)
    val ignored = expected.union(expected)
    ignored.write(dataStore, SaveMode.Ignore)
    assertResult(expected) {
      ReaderScalaEagerImpl.read[Person](dataStore)
    }
  }

  test("Writing to existing table with ignore - Spark") {
    val expected = DataSetAPI(data.toDS())
    val dataStore = HiveDataStore("default", "people_spark_ignore")
    expected.write(dataStore, SaveMode.Overwrite)
    val ignored = expected.union(expected)
    ignored.write(dataStore, SaveMode.Ignore)
    assertResult(expected) {
      ReaderSparkImpl.read[Person](dataStore)
    }
  }

  test("Writing twice to same table with append - Scala") {
    val single = DataSetAPI(data)
    val expected = single.union(single)
    val dataStore = HiveDataStore("default", "people_scala_twice")
    single.write(dataStore, SaveMode.Overwrite)
    single.write.table(dataStore, SaveMode.Append)
    assertResult(expected) {
      ReaderScalaEagerImpl.read[Person].table(dataStore)
    }
  }

  test("Writing twice to same table with append - Spark") {
    import spark.implicits._
    val single = DataSetAPI(data.toDS())
    val expected = single.union(single)
    val dataStore = HiveDataStore("default", "people_spark_twice")
    single.write(dataStore, SaveMode.Overwrite)
    single.write.table(dataStore, SaveMode.Append)
    assertResult(expected) {
      ReaderSparkImpl.read[Person].table(dataStore)
    }
  }

}