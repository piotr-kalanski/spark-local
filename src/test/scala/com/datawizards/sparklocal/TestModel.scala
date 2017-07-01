package com.datawizards.sparklocal

import com.datawizards.dmg.annotations.{column, table}
import com.datawizards.dmg.dialects
import com.datawizards.sparklocal.dataset.io.ModelDialects

object TestModel {
  case class Person(name: String, age: Int)
  case class PersonV2(name: String, age: Int, title: Option[String])
  case class PersonV3(name: String, age: Int, title: Option[String], salary: Option[Long])
  case class PersonBigInt(name: String, age: BigInt)
  case class PersonUppercase(NAME: String, AGE: Int)
  @table("PEOPLE", dialect = dialects.Hive)
  case class PersonWithMapping(
    @column("PERSON_NAME_HIVE", dialect = dialects.Hive)
    @column("PERSON_NAME_H2", dialect = dialects.H2)
    @column("PERSON_NAME_CSV", dialect = ModelDialects.CSV)
    @column("personNameJson", dialect = ModelDialects.JSON)
    @column("personNameAvro", dialect = ModelDialects.Avro)
    @column("personNameParquet", dialect = ModelDialects.Parquet)
    name: String,
    @column("PERSON_AGE_HIVE", dialect = dialects.Hive)
    @column("PERSON_AGE_H2", dialect = dialects.H2)
    @column("PERSON_AGE_CSV", dialect = ModelDialects.CSV)
    @column("personAgeJson", dialect = ModelDialects.JSON)
    @column("personAgeAvro", dialect = ModelDialects.Avro)
    @column("personAgeParquet", dialect = ModelDialects.Parquet)
    age: Int
  )
  @table("PEOPLE", dialect = dialects.Hive)
  case class PersonWithMappingV2(
                                @column("PERSON_NAME_HIVE", dialect = dialects.Hive)
                                @column("PERSON_NAME_H2", dialect = dialects.H2)
                                @column("PERSON_NAME_CSV", dialect = ModelDialects.CSV)
                                @column("personNameJson", dialect = ModelDialects.JSON)
                                @column("personNameAvro", dialect = ModelDialects.Avro)
                                @column("personNameParquet", dialect = ModelDialects.Parquet)
                                name: String,
                                @column("PERSON_AGE_HIVE", dialect = dialects.Hive)
                                @column("PERSON_AGE_H2", dialect = dialects.H2)
                                @column("PERSON_AGE_CSV", dialect = ModelDialects.CSV)
                                @column("personAgeJson", dialect = ModelDialects.JSON)
                                @column("personAgeAvro", dialect = ModelDialects.Avro)
                                @column("personAgeParquet", dialect = ModelDialects.Parquet)
                                age: Int,
                                @column("PERSON_TITLE_HIVE", dialect = dialects.Hive)
                                @column("PERSON_TITLE_H2", dialect = dialects.H2)
                                @column("PERSON_TITLE_CSV", dialect = ModelDialects.CSV)
                                @column("personTitleJson", dialect = ModelDialects.JSON)
                                @column("personTitleAvro", dialect = ModelDialects.Avro)
                                @column("personTitleParquet", dialect = ModelDialects.Parquet)
                                title: Option[String]
                              )
  @table("PEOPLE", dialect = dialects.Hive)
  case class PersonWithMappingV3(
                                  @column("PERSON_NAME_HIVE", dialect = dialects.Hive)
                                  @column("PERSON_NAME_H2", dialect = dialects.H2)
                                  @column("PERSON_NAME_CSV", dialect = ModelDialects.CSV)
                                  @column("personNameJson", dialect = ModelDialects.JSON)
                                  @column("personNameAvro", dialect = ModelDialects.Avro)
                                  @column("personNameParquet", dialect = ModelDialects.Parquet)
                                  name: String,
                                  @column("PERSON_AGE_HIVE", dialect = dialects.Hive)
                                  @column("PERSON_AGE_H2", dialect = dialects.H2)
                                  @column("PERSON_AGE_CSV", dialect = ModelDialects.CSV)
                                  @column("personAgeJson", dialect = ModelDialects.JSON)
                                  @column("personAgeAvro", dialect = ModelDialects.Avro)
                                  @column("personAgeParquet", dialect = ModelDialects.Parquet)
                                  age: Int,
                                  @column("PERSON_TITLE_HIVE", dialect = dialects.Hive)
                                  @column("PERSON_TITLE_H2", dialect = dialects.H2)
                                  @column("PERSON_TITLE_CSV", dialect = ModelDialects.CSV)
                                  @column("personTitleJson", dialect = ModelDialects.JSON)
                                  @column("personTitleAvro", dialect = ModelDialects.Avro)
                                  @column("personTitleParquet", dialect = ModelDialects.Parquet)
                                  title: Option[String],
                                  @column("PERSON_SALARY_HIVE", dialect = dialects.Hive)
                                  @column("PERSON_SALARY_H2", dialect = dialects.H2)
                                  @column("PERSON_SALARY_CSV", dialect = ModelDialects.CSV)
                                  @column("personSalaryJson", dialect = ModelDialects.JSON)
                                  @column("personSalaryAvro", dialect = ModelDialects.Avro)
                                  @column("personSalaryParquet", dialect = ModelDialects.Parquet)
                                  salary: Option[Long]
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

  implicit val peopleMappingV3Ordering = new Ordering[PersonWithMappingV3]() {
    override def compare(x: PersonWithMappingV3, y: PersonWithMappingV3): Int =
      if(x == null) -1 else if(y == null) 1 else x.name.compareTo(y.name)
  }

  implicit val booksOrdering = new Ordering[Book]() {
    override def compare(x: Book, y: Book): Int =
      if(x == null) -1 else if(y == null) 1 else x.title.compareTo(y.title)
  }
}
