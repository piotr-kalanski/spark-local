package com.datawizards.sparklocal.dataset.io

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.TestModel.Person
import com.datawizards.sparklocal.datastore.CSVDataStore
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ReadCSVTests extends SparkLocalBaseTest {

  private val peopleCSV = CSVDataStore[Person](getClass.getResource("/people.csv").getPath)
  private val people2CSV = CSVDataStore[Person](
    path = getClass.getResource("/people2.csv").getPath,
    delimiter = ';',
    header = false,
    columns = Seq("name","age")
  )

  test("Read CSV - result") {
    assertDatasetOperationResult(ReaderScalaImpl.read(peopleCSV)) {
      Array(
        Person("p1", 10),
        Person("p2", 20),
        Person("p3", 30),
        Person("p,4", 40)
      )
    }
  }

  test("Read CSV with custom options - result") {
    assertDatasetOperationResult(ReaderScalaImpl.read(people2CSV)) {
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
    assert(ReaderScalaImpl.read(peopleCSV) == ReaderSparkImpl.read(peopleCSV))
  }

  test("Read CSV with custom options - equals") {
    //access lazy val spark just to init SparkContext
    spark
    assert(ReaderScalaImpl.read(people2CSV) == ReaderSparkImpl.read(people2CSV))
  }

}