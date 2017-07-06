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

  test("Group by mean aggregation - result") {
    assertDatasetOperationResultWithSorted(peopleDataSetScala.groupByKey(_.name).agg(mean(_.age))) {
      Array(("p1", 15.0), ("p2", 35.0))
    }
  }

  test("Group by mean aggregation - equals") {
    assertDatasetOperationReturnsSameResultWithSorted(people) {
      ds => ds.groupByKey(_.name).agg(mean(_.age))
    }
  }

  test("Group by max aggregation - result") {
    assertDatasetOperationResultWithSorted(peopleDataSetScala.groupByKey(_.name).agg(max(_.age))) {
      Array(("p1", 20.0), ("p2", 40.0))
    }
  }

  test("Group by max aggregation - equals") {
    assertDatasetOperationReturnsSameResultWithSorted(people) {
      ds => ds.groupByKey(_.name).agg(max(_.age))
    }
  }

  test("Group by min aggregation - result") {
    assertDatasetOperationResultWithSorted(peopleDataSetScala.groupByKey(_.name).agg(min(_.age))) {
      Array(("p1", 10.0), ("p2", 30.0))
    }
  }

  test("Group by min aggregation - equals") {
    assertDatasetOperationReturnsSameResultWithSorted(people) {
      ds => ds.groupByKey(_.name).agg(min(_.age))
    }
  }

  test("Two aggregations - result") {
    val result = peopleDataSetScala
      .groupByKey(_.name)
      .agg(sum(_.age), count())
    assertDatasetOperationResultWithSorted(result) {
      Array(("p1", 30.0, 2L), ("p2", 70.0, 2L))
    }
  }

  test("Two aggregations - equals") {
    assertDatasetOperationReturnsSameResultWithSorted(people) {
      ds => ds
        .groupByKey(_.name)
        .agg(sum(_.age), count())
    }
  }

  test("Three aggregations - result") {
    val result = peopleDataSetScala
      .groupByKey(_.name)
      .agg(sum(_.age), count(), mean(_.age))
    assertDatasetOperationResultWithSorted(result) {
      Array(("p1", 30.0, 2L, 15.0), ("p2", 70.0, 2L, 35.0))
    }
  }

  test("Three aggregations - equals") {
    assertDatasetOperationReturnsSameResultWithSorted(people) {
      ds => ds
        .groupByKey(_.name)
        .agg(sum(_.age), count(), mean(_.age))
    }
  }

  test("Four aggregations - result") {
    val result = peopleDataSetScala
      .groupByKey(_.name)
      .agg(sum(_.age), count(), mean(_.age), max(_.age))
    assertDatasetOperationResultWithSorted(result) {
      Array(("p1", 30.0, 2L, 15.0, 20.0), ("p2", 70.0, 2L, 35.0, 40.0))
    }
  }

  test("Four aggregations - equals") {
    assertDatasetOperationReturnsSameResultWithSorted(people) {
      ds => ds
        .groupByKey(_.name)
        .agg(sum(_.age), count(), mean(_.age), max(_.age))
    }
  }
}