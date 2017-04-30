package com.datawizards.sparklocal.rdd

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FilterTest extends SparkLocalBaseTest {

  test("Filter result") {
    assertRDDOperationResult(
      RDDAPI(Seq(1,2,3)).filter(_ % 2 == 0)
    ) {
      Array(2)
    }
  }

  test("Filter equal") {
    assertRDDOperation(Seq(1,2,3)){
      ds => ds.filter(_ % 2 == 0)
    }
  }

}