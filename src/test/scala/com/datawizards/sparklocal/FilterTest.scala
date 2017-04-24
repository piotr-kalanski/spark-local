package com.datawizards.sparklocal

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FilterTest extends SparkLocalBaseTest {

  test("Filter result") {
    assertDatasetOperationResult(
      DataSetAPI(Seq(1,2,3)).filter(_ % 2 == 0)
    ) {
      Array(2)
    }
  }

  test("Filter equal") {
    assertDatasetOperation(Seq(1,2,3)){
      ds => ds.filter(_ % 2 == 0)
    }
  }

}