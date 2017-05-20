package com.datawizards.sparklocal.dataset.io.jdbc2class

import java.sql.DriverManager

import com.datawizards.sparklocal.TestModel.Person
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Jdbc2ClassTest extends FunSuite {

  test("Select table") {
    Class.forName("org.h2.Driver")
    val connectionString = "jdbc:h2:mem:test"
    val connectionProperties = new java.util.Properties
    val connection = DriverManager.getConnection(connectionString, "", "")
    connection.createStatement().execute("create table people(name VARCHAR, age INT)")
    connection.createStatement().execute("insert into people values('r1', 1)")
    connection.createStatement().execute("insert into people values('r2', 2)")

    val result = selectTable[Person](connection, "people")._1
    val expected = Seq(Person("r1", 1), Person("r2", 2))
    assertResult(expected)(result)

    connection.close()
  }

}
