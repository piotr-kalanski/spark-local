package com.datawizards.sparklocal.dataset.expressions

import com.datawizards.sparklocal.dataset.expressions.Expressions._
import com.datawizards.sparklocal.TestModel._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ExpressionsAPITest extends FunSuite {

  val p1 = Person("p1", 1)
  val p2 = Person("p2", 2)
  val b1 = Book("book1", 2000, "p1")
  val b2 = Book("book2", 2001, "p2")

  test("literal values") {
    assert(new Literal(1).eval(p1,p2) == 1)
    assert(new Literal("asd").eval(p1,p2) == "asd")
  }

  test("field value") {
    assert(new Field("name").eval(p1,b1) == "p1")
    assert(new Field("age").eval(b1,p1) == 1)
  }

  test("eq") {
    assert((new Literal(1) === new Literal(1)).eval(p1,p2))
    assert(!(new Literal(1) === new Literal(2)).eval(p1,p2))
    assert((new Field("name") === new Field("personName")).eval(p1,b1))
    assert((new Field("name") =!= new Field("personName")).eval(p1,b2))
  }

  test("and") {
    assert(((new Field("name") === new Field("personName")) && (new Field("year") === new Literal(2000))).eval(p1,b1))
  }

  test("or") {
    assert(((new Field("name") =!= new Field("personName")) || (new Field("year") === new Literal(2000))).eval(p1,b1))
  }

  test("not") {
    assert((!(new Literal(1) === new Literal(2))).eval(p1,p2))
  }

}
