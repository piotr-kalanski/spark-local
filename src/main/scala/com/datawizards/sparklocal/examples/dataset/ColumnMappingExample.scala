package com.datawizards.sparklocal.examples.dataset

import com.datawizards.dmg.annotations.column
import com.datawizards.sparklocal.dataset.io.ModelDialects
import com.datawizards.sparklocal.datastore.JsonDataStore
import com.datawizards.sparklocal.session.{ExecutionEngine, SparkSessionAPI}
import org.apache.spark.sql.SaveMode

object ColumnMappingExample extends App {
  case class PersonWithMapping(
                                @column("personName", dialect = ModelDialects.JSON)
                                name: String,
                                @column("personAge", dialect = ModelDialects.JSON)
                                age: Int
                              )

  val session = SparkSessionAPI
    .builder(ExecutionEngine.ScalaEager)
    .master("local")
    .getOrCreate()

  import session.implicits._

  val people = session.createDataset(Seq(
    PersonWithMapping("p1", 10),
    PersonWithMapping("p2", 20),
    PersonWithMapping("p3", 30),
    PersonWithMapping("p4", 40)
  ))

  val dataStore = JsonDataStore("people_mapping.json")
  people.write(dataStore, SaveMode.Overwrite)

  val people2 = session.read[PersonWithMapping](dataStore)
  people2.show()
}
