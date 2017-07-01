package com.datawizards.sparklocal.examples.dataset

import com.datawizards.dmg.annotations.{column, table}
import com.datawizards.dmg.dialects
import com.datawizards.sparklocal.dataset.io.ModelDialects

object Model {
  case class Person(id: Int, name: String, gender: String)
  case class WorkExperience(personId: Int, year: Int, title: String)
  case class HRReport(year: Int, title: String, gender: String, count: Int)
  @table("PEOPLE", dialect = dialects.Hive)
  case class PersonWithMapping(
                                @column("PERSON_NAME", dialect = dialects.Hive)
                                @column("PERSON_NAME", dialect = ModelDialects.CSV)
                                @column("personName", dialect = ModelDialects.JSON)
                                @column("personName", dialect = ModelDialects.Avro)
                                @column("personName", dialect = ModelDialects.Parquet)
                                name: String,
                                @column("PERSON_AGE", dialect = dialects.Hive)
                                @column("PERSON_AGE", dialect = ModelDialects.CSV)
                                @column("personAge", dialect = ModelDialects.JSON)
                                @column("personAge", dialect = ModelDialects.Avro)
                                @column("personAge", dialect = ModelDialects.Parquet)
                                age: Int
                              )
}