package com.datawizards.sparklocal.dataset.grouped

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.implicits._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CountTest extends SparkLocalBaseTest {

  val data = Seq(("a",10),("b",1),("b",23),("c",0),("c",1))

  test("Count result") {
    assertDatasetOperationResultWithSorted(DataSetAPI(data).groupByKey(_._1).count()) {
      Array(("a",1),("b",2),("c",2))
    }
  }

  test("Count equal") {
    assertDatasetOperationReturnsSameResultWithSorted(data) {
      ds => ds.groupByKey(_._1).count()
    }
  }

}