package com.datawizards.sparklocal.dataset.io

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.TestModel.Person
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.datastore.AvroDataStore
import com.datawizards.sparklocal.impl.scala.eager.dataset.io.ReaderScalaImpl
import com.datawizards.sparklocal.impl.spark.dataset.io.ReaderSparkImpl
import com.datawizards.sparklocal.implicits._
import org.apache.spark.sql.SaveMode
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class WriteReadAvroTest extends SparkLocalBaseTest {

  val data = Seq(
    Person("p1", 10),
    Person("p2", 20),
    Person("p3", 30),
    Person("p4", 40)
  )

  test("Writing and reading avro file produces the same result - Scala") {
    val file = "target/people_scala.avro"
    val expected = DataSetAPI(data)
    val dataStore = AvroDataStore(file)
    expected.write(dataStore, SaveMode.Overwrite)
    assertResult(expected) {
      ReaderScalaImpl.read[Person](dataStore)
    }
  }

  test("Writing and reading avro file produces the same result - Spark") {
    import spark.implicits._
    val file = "target/people_spark.avro"
    val expected = DataSetAPI(data.toDS())
    val dataStore = AvroDataStore(file)
    expected.write(dataStore, SaveMode.Overwrite)
    assertResult(expected) {
      ReaderSparkImpl.read[Person](dataStore)
    }
  }

  test("Trying to write to existing file should throw exception - Scala") {
    val file = "target/people_scala_error.avro"
    val expected = DataSetAPI(data)
    val dataStore = AvroDataStore(file)
    expected.write(dataStore, SaveMode.Overwrite)
    intercept[Exception] {
      expected.write(dataStore, SaveMode.ErrorIfExists)
    }
  }

  test("Trying to write to existing file should throw exception - Spark") {
    import spark.implicits._
    val file = "target/people_spark_error.avro"
    val expected = DataSetAPI(data.toDS())
    val dataStore = AvroDataStore(file)
    expected.write(dataStore, SaveMode.Overwrite)
    intercept[Exception] {
      expected.write(dataStore, SaveMode.ErrorIfExists)
    }
  }

  test("Writing twice to same file produces single result - Scala") {
    val file = "target/people_scala_twice.avro"
    val expected = DataSetAPI(data)
    val dataStore = AvroDataStore(file)
    expected.union(expected).write(dataStore, SaveMode.Overwrite)
    expected.write(dataStore, SaveMode.Overwrite)
    assertResult(expected) {
      ReaderScalaImpl.read[Person](dataStore)
    }
  }

  test("Writing twice to same file produces single result - Spark") {
    import spark.implicits._
    val file = "target/people_spark_twice.avro"
    val expected = DataSetAPI(data.toDS())
    val dataStore = AvroDataStore(file)
    expected.union(expected).write(dataStore, SaveMode.Overwrite)
    expected.write(dataStore, SaveMode.Overwrite)
    assertResult(expected) {
      ReaderSparkImpl.read[Person](dataStore)
    }
  }

  test("Writing to existing file with ignore - Scala") {
    val file = "target/people_scala_ignore.avro"
    val expected = DataSetAPI(data)
    val dataStore = AvroDataStore(file)
    expected.write(dataStore, SaveMode.Overwrite)
    val ignored = expected.union(expected)
    ignored.write(dataStore, SaveMode.Ignore)
    assertResult(expected) {
      ReaderScalaImpl.read[Person](dataStore)
    }
  }

  test("Writing to existing file with ignore - Spark") {
    val file = "target/people_spark_ignore.avro"
    val expected = DataSetAPI(data.toDS())
    val dataStore = AvroDataStore(file)
    expected.write(dataStore, SaveMode.Overwrite)
    val ignored = expected.union(expected)
    ignored.write(dataStore, SaveMode.Ignore)
    assertResult(expected) {
      ReaderSparkImpl.read[Person](dataStore)
    }
  }

  test("Writing twice to same file with append - Scala") {
    val file = "target/people_scala_twice.avro"
    val single = DataSetAPI(data)
    val expected = single.union(single)
    val dataStore = AvroDataStore(file)
    single.write(dataStore, SaveMode.Overwrite)
    single.write.avro(dataStore, SaveMode.Append)
    assertResult(expected) {
      ReaderScalaImpl.read[Person].avro(dataStore)
    }
  }

  test("Writing twice to same file with append - Spark") {
    import spark.implicits._
    val file = "target/people_spark_twice.avro"
    val single = DataSetAPI(data.toDS())
    val expected = single.union(single)
    val dataStore = AvroDataStore(file)
    single.write(dataStore, SaveMode.Overwrite)
    single.write.avro(dataStore, SaveMode.Append)
    assertResult(expected) {
      ReaderSparkImpl.read[Person].avro(dataStore)
    }
  }

}