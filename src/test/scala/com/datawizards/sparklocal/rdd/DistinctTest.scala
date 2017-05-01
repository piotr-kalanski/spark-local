package com.datawizards.sparklocal.rdd

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DistinctTest extends SparkLocalBaseTest {

  test("Distinct result") {
    assertRDDOperationResult(
      RDDAPI(Seq(1,1,2,3,2,3,1)).distinct()
    ) {
      Array(1,2,3)
    }
  }

  test("Distinct equal") {
    assertRDDOperationWithSortedResult(Seq(1,1,2,3,2,3,1)){
      ds => ds.distinct()
    }
  }

}