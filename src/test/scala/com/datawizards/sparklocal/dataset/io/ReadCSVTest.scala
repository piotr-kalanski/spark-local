package com.datawizards.sparklocal.dataset.io

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.TestModel.Person
import com.datawizards.sparklocal.datastore.CSVDataStore
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
    assertDatasetOperationResult(ReaderScalaImpl.read[Person](peopleCSV)) {
      Array(
        Person("p1", 10),
        Person("p2", 20),
        Person("p3", 30),
        Person("p,4", 40)
      )
    }
  }

  test("Read CSV with custom options - result") {
    assertDatasetOperationResult(ReaderScalaImpl.read[Person](people2CSV)) {
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
    assert(ReaderScalaImpl.read[Person](peopleCSV) == ReaderSparkImpl.read[Person](peopleCSV))
  }

  test("Read CSV with custom options - equals") {
    //access lazy val spark just to init SparkContext
    spark
    assert(ReaderScalaImpl.read[Person](people2CSV) == ReaderSparkImpl.read[Person](people2CSV))
  }

}