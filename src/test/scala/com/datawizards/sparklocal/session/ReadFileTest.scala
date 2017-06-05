package com.datawizards.sparklocal.session

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.TestModel.{Person, PersonBigInt}
import com.datawizards.sparklocal.datastore.{CSVDataStore, JsonDataStore}
import com.datawizards.sparklocal.implicits._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ReadFileTest extends SparkLocalBaseTest {

  test("read CSV") {
    testReadCSV(ExecutionEngine.ScalaEager)
    testReadCSV(ExecutionEngine.ScalaLazy)
    testReadCSV(ExecutionEngine.Spark)
  }

  test("read JSON") {
    testReadJson(ExecutionEngine.ScalaEager)
    testReadJson(ExecutionEngine.ScalaLazy)
    testReadJson(ExecutionEngine.Spark)
  }


  private def testReadCSV[Session <: SparkSessionAPI](engine: ExecutionEngine[Session]): Unit = {
    val peopleCSV = CSVDataStore(getClass.getResource("/people.csv").getPath)

    val session = SparkSessionAPI
      .builder(engine)
      .master("local")
      .getOrCreate()

    assertDatasetOperationResult(session.read[Person](peopleCSV)) {
      Array(
        Person("p1", 10),
        Person("p2", 20),
        Person("p3", 30),
        Person("p,4", 40)
      )
    }
  }

  private def testReadJson[Session <: SparkSessionAPI](engine: ExecutionEngine[Session]): Unit = {
    val peopleJson = JsonDataStore(getClass.getResource("/people.json").getPath)

    val session = SparkSessionAPI
      .builder(engine)
      .master("local")
      .getOrCreate()

    assertDatasetOperationResult(session.read[PersonBigInt](peopleJson)) {
      Array(
        PersonBigInt("p1", 10),
        PersonBigInt("p2", 20),
        PersonBigInt("p3", 30),
        PersonBigInt("p,4", 40)
      )
    }
  }

}