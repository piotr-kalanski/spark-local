package com.datawizards.sparklocal.dataset.grouped

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.dataset.DataSetAPI
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FlatMapGroupsTest extends SparkLocalBaseTest {

  val data = Seq(("a",1),("b",2),("b",3),("c",10),("c",20))

  test("flatMapGroups result") {
    val result = DataSetAPI(data)
      .groupByKey(_._1)
      .flatMapGroups{case (_,vals) => vals.map(x => x._2)}

    assertDatasetOperationResultWithSorted(result) {
      Array(1,2,3,10,20)
    }
  }

  test("flatMapGroups equal") {
    assertDatasetOperationReturnsSameResultWithSorted(data) {
      ds => ds.groupByKey(_._1).flatMapGroups{case (_,vals) => vals.map(x => x._2)}
    }
  }

}