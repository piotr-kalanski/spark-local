package com.datawizards.sparklocal.dataset

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class LimitTest extends SparkLocalBaseTest {

  test("Limit(n) result") {
    assertDatasetOperationResult(DataSetAPI(Seq(1,2,3)).limit(2)) {
      Array(1,2)
    }
  }

  test("Limit(n) equal") {
    assertDatasetOperationReturnsSameResultWithSorted(Seq(1,2,3)) {
      ds => ds.limit(2)
    }
  }

}