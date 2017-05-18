package com.datawizards.sparklocal.dataset.grouped

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.implicits._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ReduceGroupsTest extends SparkLocalBaseTest {

  val data = Seq(("a",1),("b",2),("b",3),("c",10),("c",20))

  test("mapValues and mapGroups result") {
    val result = DataSetAPI(data)
      .groupByKey(_._1)
      .mapValues(v => v._2)
      .reduceGroups(_+_)

    assertDatasetOperationResultWithSorted(result) {
      Array(("a",1),("b",5),("c",30))
    }
  }

  test("mapValues and mapGroups equal") {
    assertDatasetOperationReturnsSameResultWithSorted(data) {
      ds => ds.groupByKey(_._1).mapValues(v => v._2).reduceGroups(_+_)
    }
  }

}