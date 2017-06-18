package com.datawizards.sparklocal

object TestModel {
  case class Person(name: String, age: Int)
  case class PersonV2(name: String, age: Int, title: Option[String])
  case class PersonV3(name: String, age: Int, title: Option[String], salary: Option[Long])
  case class PersonBigInt(name: String, age: BigInt)
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

  implicit val booksOrdering = new Ordering[Book]() {
    override def compare(x: Book, y: Book): Int =
      if(x == null) -1 else if(y == null) 1 else x.title.compareTo(y.title)
  }
}
