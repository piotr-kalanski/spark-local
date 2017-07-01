package com.datawizards.sparklocal

import com.datawizards.dmg.annotations.{column, table}
import com.datawizards.dmg.dialects
import com.datawizards.sparklocal.dataset.io.ModelDialects

object TestModel {
  case class Person(name: String, age: Int)
  case class PersonV2(name: String, age: Int, title: Option[String])
  case class PersonV3(name: String, age: Int, title: Option[String], salary: Option[Long])
  case class PersonBigInt(name: String, age: BigInt)

  @table("PEOPLE", dialect = dialects.Hive)
  case class PersonWithMapping(
    @column("PERSON_NAME_HIVE", dialect = dialects.Hive)
    @column("PERSON_NAME_CSV", dialect = ModelDialects.CSV)
    @column("personNameJson", dialect = ModelDialects.JSON)
    @column("personNameAvro", dialect = ModelDialects.Avro)
    @column("personNameParquet", dialect = ModelDialects.Parquet)
    name: String,
    @column("PERSON_AGE_HIVE", dialect = dialects.Hive)
    @column("PERSON_AGE_CSV", dialect = ModelDialects.CSV)
    @column("personAgeJson", dialect = ModelDialects.JSON)
    @column("personAgeAvro", dialect = ModelDialects.Avro)
    @column("personAgeParquet", dialect = ModelDialects.Parquet)
    age: Int
  )

  case class Book(title: String, year: Int, personName: String)
  case class LargeClass(strVal   : String,
                        intVal   : Int,
                        longVal  : Long,
                        doubleVal: Double,
                        floatVal : Float,
                        shortVal : Short,
                        flag     : Boolean,
                        byteVal  : Byte)

  implicit val peopleOrdering = new Ordering[Person]() {
    override def compare(x: Person, y: Person): Int =
      if(x == null) -1 else if(y == null) 1 else x.name.compareTo(y.name)
  }

  implicit val peopleBigIntOrdering = new Ordering[PersonBigInt]() {
    override def compare(x: PersonBigInt, y: PersonBigInt): Int =
      if(x == null) -1 else if(y == null) 1 else x.name.compareTo(y.name)
  }

  implicit val peopleV3Ordering = new Ordering[PersonV3]() {
    override def compare(x: PersonV3, y: PersonV3): Int =
      if(x == null) -1 else if(y == null) 1 else x.name.compareTo(y.name)
  }

  implicit val peopleMappingOrdering = new Ordering[PersonWithMapping]() {
    override def compare(x: PersonWithMapping, y: PersonWithMapping): Int =
      if(x == null) -1 else if(y == null) 1 else x.name.compareTo(y.name)
  }

  implicit val booksOrdering = new Ordering[Book]() {
    override def compare(x: Book, y: Book): Int =
      if(x == null) -1 else if(y == null) 1 else x.title.compareTo(y.title)
  }
}
