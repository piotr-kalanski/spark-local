package com.datawizards.sparklocal.dataset.io

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.TestModel.Person
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.datastore.JsonDataStore
import com.datawizards.sparklocal.impl.scala.eager.dataset.io.ReaderScalaEagerImpl
import com.datawizards.sparklocal.impl.spark.dataset.io.ReaderSparkImpl
import com.datawizards.sparklocal.implicits._
import org.apache.spark.sql.SaveMode
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class WriteReadJsonTest extends SparkLocalBaseTest {

  val data = Seq(
    Person("p1", 10),
    Person("p2", 20),
    Person("p3", 30),
    Person("p4", 40)
  )

  test("Writing and reading file produces the same result - Scala") {
    val file = "target/people_scala.json"
    val expected = DataSetAPI(data)
    val dataStore = JsonDataStore(file)
    expected.write(dataStore, SaveMode.Overwrite)
    assertResult(expected) {
      ReaderScalaEagerImpl.read[Person](dataStore)
    }
  }

  test("Writing and reading file produces the same result - Spark") {
    import spark.implicits._
    val file = "target/people_spark.json"
    val expected = DataSetAPI(data.toDS())
    val dataStore = JsonDataStore(file)
    expected.write(dataStore, SaveMode.Overwrite)
    assertResult(expected) {
      ReaderSparkImpl.read[Person](dataStore)
    }
  }

  test("Trying to write to existing file should throw exception - Scala") {
    val file = "target/people_scala_error.json"
    val expected = DataSetAPI(data)
    val dataStore = JsonDataStore(file)
    expected.write(dataStore, SaveMode.Overwrite)
    intercept[Exception] {
      expected.write(dataStore, SaveMode.ErrorIfExists)
    }
  }

  test("Trying to write to existing file should throw exception - Spark") {
    import spark.implicits._
    val file = "target/people_spark_error.json"
    val expected = DataSetAPI(data.toDS())
    val dataStore = JsonDataStore(file)
    expected.write(dataStore, SaveMode.Overwrite)
    intercept[Exception] {
      expected.write(dataStore, SaveMode.ErrorIfExists)
    }
  }

  test("Writing twice to same file produces single result - Scala") {
    val file = "target/people_scala_twice.json"
    val expected = DataSetAPI(data)
    val dataStore = JsonDataStore(file)
    expected.union(expected).write(dataStore, SaveMode.Overwrite)
    expected.write(dataStore, SaveMode.Overwrite)
    assertResult(expected) {
      ReaderScalaEagerImpl.read[Person](dataStore)
    }
  }

  test("Writing twice to same file produces single result - Spark") {
    import spark.implicits._
    val file = "target/people_spark_twice.json"
    val expected = DataSetAPI(data.toDS())
    val dataStore = JsonDataStore(file)
    expected.union(expected).write(dataStore, SaveMode.Overwrite)
    expected.write(dataStore, SaveMode.Overwrite)
    assertResult(expected) {
      ReaderSparkImpl.read[Person](dataStore)
    }
  }

  test("Writing to existing file with ignore - Scala") {
    val file = "target/people_scala_ignore.json"
    val expected = DataSetAPI(data)
    val dataStore = JsonDataStore(file)
    expected.write(dataStore, SaveMode.Overwrite)
    val ignored = expected.union(expected)
    ignored.write(dataStore, SaveMode.Ignore)
    assertResult(expected) {
      ReaderScalaEagerImpl.read[Person](dataStore)
    }
  }

  test("Writing to existing file with ignore - Spark") {
    val file = "target/people_spark_ignore.json"
    val expected = DataSetAPI(data.toDS())
    val dataStore = JsonDataStore(file)
    expected.write(dataStore, SaveMode.Overwrite)
    val ignored = expected.union(expected)
    ignored.write(dataStore, SaveMode.Ignore)
    assertResult(expected) {
      ReaderSparkImpl.read[Person](dataStore)
    }
  }

  test("Writing twice to same file with append - Scala") {
    val file = "target/people_scala_twice.json"
    val single = DataSetAPI(data)
    val expected = single.union(single)
    val dataStore = JsonDataStore(file)
    single.write(dataStore, SaveMode.Overwrite)
    single.write.json(dataStore, SaveMode.Append)
    assertResult(expected) {
      ReaderScalaEagerImpl.read[Person].json(dataStore)
    }
  }

  test("Writing twice to same file with append - Spark") {
    import spark.implicits._
    val file = "target/people_spark_twice.json"
    val single = DataSetAPI(data.toDS())
    val expected = single.union(single)
    val dataStore = JsonDataStore(file)
    single.write(dataStore, SaveMode.Overwrite)
    single.write.json(dataStore, SaveMode.Append)
    assertResult(expected) {
      ReaderSparkImpl.read[Person].json(dataStore)
    }
  }

  test("Writing with partitioning - Spark") {
    import spark.implicits._
    val file = "target/people_partitioned_spark.json"
    val ds = DataSetAPI(data.toDS())
    val expected = data.toArray
    val dataStore = JsonDataStore(file)
    ds.write.partitionBy("age")(dataStore, SaveMode.Overwrite)
    assertDatasetOperationResultWithSorted(ReaderSparkImpl.read[Person](dataStore)) {
      expected
    }
    assertResult(Seq("age=10","age=20","age=30","age=40","._SUCCESS.crc","_SUCCESS").sorted) {
      listFilesInDirectory(file).toSeq.sorted
    }
  }

}