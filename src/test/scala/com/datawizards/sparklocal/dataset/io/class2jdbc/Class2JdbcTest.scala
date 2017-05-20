package com.datawizards.sparklocal.dataset.io.class2jdbc

import com.datawizards.sparklocal.TestModel.Person
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Class2JdbcTest extends FunSuite {

  test("Generate inserts") {
    val data = Seq(Person("p1", 1), Person("p2", 2))

    val expected = Seq(
      """INSERT INTO Person(name,age) VALUES('p1',1)""",
      """INSERT INTO Person(name,age) VALUES('p2',2)"""
    )

    assertResult(expected) { generateInserts(data) }
  }

  test("Generate inserts - custom table name") {
    val data = Seq(Person("p1", 1), Person("p2", 2))

    val expected = Seq(
      """INSERT INTO PEOPLE(name,age) VALUES('p1',1)""",
      """INSERT INTO PEOPLE(name,age) VALUES('p2',2)"""
    )

    assertResult(expected) { generateInserts(data, "PEOPLE") }
  }

  test("Generate inserts - custom table name and columns") {
    val data = Seq(Person("p1", 1), Person("p2", 2))

    val expected = Seq(
      """INSERT INTO PEOPLE(person_name,person_age) VALUES('p1',1)""",
      """INSERT INTO PEOPLE(person_name,person_age) VALUES('p2',2)"""
    )

    assertResult(expected) { generateInserts(data, "PEOPLE", Seq("person_name", "person_age")) }
  }

}
