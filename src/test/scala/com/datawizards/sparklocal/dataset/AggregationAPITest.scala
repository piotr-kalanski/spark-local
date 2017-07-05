package com.datawizards.sparklocal.dataset

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.TestModel.Person
import com.datawizards.sparklocal.implicits._
import com.datawizards.sparklocal.dataset.agg._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AggregationAPITest extends SparkLocalBaseTest {

  val people = Seq(
    Person("p1", 10),
    Person("p1", 20),
    Person("p2", 30),
    Person("p2", 40)
  )

  lazy val peopleDataSetScala = DataSetAPI(people)
  lazy val peopleDataSetSpark = DataSetAPI(people.toDS())

  /*test("Sum aggregation - result") {
    assertDatasetOperationResult(peopleDataSetScala.agg(sum(_.age))) {
      Array(100.0)
    }
  }

  test("Sum aggregation - equals") {
    assertDatasetOperationReturnsSameResultWithSorted(people) {
      ds => ds.agg(sum(_.age))
    }
  }*/

  test("Group by sum aggregation - result") {
    assertDatasetOperationResultWithSorted(peopleDataSetScala.groupByKey(_.name).agg(sum(_.age))) {
      Array(("p1", 30.0), ("p2", 70.0))
    }
  }

  test("Group by sum aggregation - equals") {
    assertDatasetOperationReturnsSameResultWithSorted(people) {
      ds => ds.groupByKey(_.name).agg(sum(_.age))
    }
  }

  test("Group by count aggregation - result") {
    assertDatasetOperationResultWithSorted(peopleDataSetScala.groupByKey(_.name).agg(count())) {
      Array(("p1", 2L), ("p2", 2L))
    }
  }

  test("Group by count aggregation - equals") {
    assertDatasetOperationReturnsSameResultWithSorted(people) {
      ds => ds.groupByKey(_.name).agg(count())
    }
  }
}