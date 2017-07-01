package com.datawizards.sparklocal.examples.dataset

import com.datawizards.sparklocal.datastore.CSVDataStore
import com.datawizards.sparklocal.session.{ExecutionEngine, SparkSessionAPI}
import org.apache.spark.sql.SaveMode

object VersioningExample extends App {
  case class Person(name: String, age: Int)
  case class PersonV2(name: String, age: Int, title: Option[String])
  case class PersonV3(name: String, age: Int, title: Option[String], salary: Option[Long])

  val session = SparkSessionAPI
    .builder(ExecutionEngine.ScalaEager)
    .master("local")
    .getOrCreate()

  import session.implicits._

  val peopleV2 = session.createDataset(Seq(
    PersonV2("p1", 10, Some("Mr")),
    PersonV2("p2", 20, Some("Ms")),
    PersonV2("p3", 30, None),
    PersonV2("p4", 40, Some("Mr"))
  ))

  val dataStore = CSVDataStore("people_v2.csv")
  peopleV2.write(dataStore, SaveMode.Overwrite)

  // Read old version:
  val peopleV1 = session.read[Person](dataStore)

  // Read new version:
  val peopleV3 = session.read[PersonV3](dataStore)

  peopleV1.show()
  peopleV3.show()
}
