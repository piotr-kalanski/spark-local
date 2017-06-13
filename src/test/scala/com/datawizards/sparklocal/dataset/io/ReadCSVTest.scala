package com.datawizards.sparklocal.dataset.io

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.TestModel.Person
import com.datawizards.sparklocal.datastore.CSVDataStore
import com.datawizards.sparklocal.impl.scala.`lazy`.dataset.io.ReaderScalaLazyImpl
import com.datawizards.sparklocal.impl.scala.eager.dataset.io.ReaderScalaEagerImpl
import com.datawizards.sparklocal.impl.scala.parallel.dataset.io.ReaderScalaParallelImpl
import com.datawizards.sparklocal.impl.spark.dataset.io.ReaderSparkImpl
import com.datawizards.sparklocal.implicits._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ReadCSVTest extends SparkLocalBaseTest {

  private val peopleCSV = CSVDataStore(getClass.getResource("/people.csv").getPath)
  private val people2CSV = CSVDataStore(
    path = getClass.getResource("/people2.csv").getPath,
    delimiter = ';',
    header = false,
    columns = Seq("name","age")
  )

  test("Read CSV - result") {
    assertDatasetOperationResult(ReaderScalaEagerImpl.read[Person](peopleCSV)) {
      Array(
        Person("p1", 10),
        Person("p2", 20),
        Person("p3", 30),
        Person("p,4", 40)
      )
    }
  }

  test("Read CSV with custom options - result") {
    assertDatasetOperationResult(ReaderScalaEagerImpl.read[Person](people2CSV)) {
      Array(
        Person("p1", 10),
        Person("p2", 20),
        Person("p3", 30),
        Person("p;4", 40)
      )
    }
  }

  test("Read CSV - equals") {
    //access lazy val spark just to init SparkContext
    spark
    val scalaEagerDs = ReaderScalaEagerImpl.read[Person](peopleCSV)
    val scalaLazyDs = ReaderScalaLazyImpl.read[Person](peopleCSV)
    val scalaParallelDs = ReaderScalaParallelImpl.read[Person](peopleCSV)
    val sparkDs = ReaderSparkImpl.read[Person](peopleCSV)

    assertDatasetEquals(scalaEagerDs, scalaLazyDs)
    assertDatasetEquals(scalaEagerDs, scalaParallelDs)
    assertDatasetEquals(scalaEagerDs, sparkDs)
  }

  test("Read CSV with custom options - equals") {
    //access lazy val spark just to init SparkContext
    spark
    assert(ReaderScalaEagerImpl.read[Person](people2CSV) == ReaderSparkImpl.read[Person](people2CSV))
  }

}