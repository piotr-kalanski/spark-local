package com.datawizards.sparklocal.dataset.grouped

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.implicits._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MapValuesAndMapGroupsTest extends SparkLocalBaseTest {

  val data = Seq(("a",1),("b",2),("b",3),("c",10),("c",20))

  test("mapGroups result") {
    val result = DataSetAPI(data)
      .groupByKey(_._1)
      .mapGroups{case (k,values) => (k,values.map(_._2).sum)}

    assertDatasetOperationResultWithSorted(result) {
      Array(("a",1),("b",5),("c",30))
    }
  }

  test("mapGroups equal") {
    assertDatasetOperationReturnsSameResultWithSorted(data) {
      ds => ds.groupByKey(_._1).mapGroups{case (k,values) => (k,values.map(_._2).sum)}
    }
  }

}