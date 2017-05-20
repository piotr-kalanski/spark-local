package com.datawizards.sparklocal.dataset

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.implicits._
import com.datawizards.sparklocal.dataset.expressions.Expressions.Literal
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.datawizards.sparklocal.TestModel._

@RunWith(classOf[JUnitRunner])
class JoinsCustomAPITest extends SparkLocalBaseTest {

  val p1 = Person("p1",1)
  val p2 = Person("p2",2)
  val p3 = Person("p3",3)

  val b1 = Book("book1", 2001, "p1")
  val b2 = Book("book2", 2002, "p2")
  val b4 = Book("book4", 2004, "p4")

  val people = Seq(p1,p2,p3)
  val books = Seq(b1,b2,b4)

  val peopleDs = DataSetAPI(people)
  val booksDs = DataSetAPI(books)

  test("join result") {
    assertDatasetOperationResult(peopleDs.join(booksDs, peopleDs("name") === booksDs("personName"))) {
     Array(
       (p1,b1),
       (p2,b2)
     )
    }
  }

  test("left join result") {
    assertDatasetOperationResult(peopleDs.leftOuterJoin(booksDs, peopleDs("name") === booksDs("personName"))) {
      Array(
        (p1,b1),
        (p2,b2),
        (p3,null)
      )
    }
  }

  test("right join result") {
    assertDatasetOperationResult(peopleDs.rightOuterJoin(booksDs, peopleDs("name") === booksDs("personName"))) {
      Array(
        (p1,b1),
        (p2,b2),
        (null,b4)
      )
    }
  }

  test("full outer join result") {
    assertDatasetOperationResult(peopleDs.fullOuterJoin(booksDs, peopleDs("name") === booksDs("personName"))) {
      Array(
        (p1,b1),
        (p2,b2),
        (p3,null),
        (null,b4)
      )
    }
  }

  test("join - Scala, Spark - equals") {
    assertDatasetOperationReturnsSameResultWithSorted(people) {
      ds => ds.join(booksDs, ds("name") === booksDs("personName"))
    }
  }

  test("left join - Scala, Spark  - equals") {
    assertDatasetOperationReturnsSameResultWithSorted(people){
      ds => ds.leftOuterJoin(booksDs, ds("name") === booksDs("personName"))
    }
  }

  test("right join - Scala, Spark  - equals") {
    assertDatasetOperationReturnsSameResultWithSorted(people){
      ds => ds.rightOuterJoin(booksDs, ds("name") === booksDs("personName"))
    }
  }

  test("full outer join - Scala, Spark  - equals") {
    assertDatasetOperationReturnsSameResultWithSorted(people){
      ds => ds.fullOuterJoin(booksDs, ds("name") === booksDs("personName"))
    }
  }

  test("join - Spark, Scala - equals") {
    assertDatasetOperationReturnsSameResultWithSorted(people){
      ds => booksDs.join(ds, booksDs("personName") === ds("name"))
    }
  }

  test("left join - Spark, Scala  - equals") {
    assertDatasetOperationReturnsSameResultWithSorted(people){
      ds => booksDs.leftOuterJoin(ds, booksDs("personName") === ds("name"))
    }
  }

  test("right join - Spark, Scala  - equals") {
    assertDatasetOperationReturnsSameResultWithSorted(people){
      ds => booksDs.rightOuterJoin(ds, booksDs("personName") === ds("name"))
    }
  }

  test("full outer join - Spark, Scala  - equals") {
    assertDatasetOperationReturnsSameResultWithSorted(people){
      ds => booksDs.fullOuterJoin(ds, booksDs("personName") === ds("name"))
    }
  }

  test("join with complex expression - Scala, Spark - equals") {
    assertDatasetOperationReturnsSameResultWithSorted(people) {
      ds => ds.join(booksDs, (ds("name") === booksDs("personName")) || !(ds("name") === new Literal("p2")) && (booksDs("personName") === new Literal("p1")))
    }
  }

}