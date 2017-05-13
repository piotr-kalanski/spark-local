package com.datawizards.sparklocal

object TestModel {
  case class Person(name: String, age: Int)
  case class PersonBigInt(name: String, age: BigInt)
  case class Book(title: String, year: Int, personName: String)

  implicit val peopleOrdering = new Ordering[Person]() {
    override def compare(x: Person, y: Person): Int =
      if(x == null) -1 else if(y == null) 1 else x.name.compareTo(y.name)
  }

  implicit val booksOrdering = new Ordering[Book]() {
    override def compare(x: Book, y: Book): Int =
      if(x == null) -1 else if(y == null) 1 else x.title.compareTo(y.title)
  }
}
