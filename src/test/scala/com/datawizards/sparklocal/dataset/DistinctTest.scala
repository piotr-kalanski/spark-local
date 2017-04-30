package com.datawizards.sparklocal.dataset

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DistinctTest extends SparkLocalBaseTest {

  test("Distinct result") {
    assertDatasetOperationResult(
      DataSetAPI(Seq(1,1,2,2,3)).distinct()
    ) {
      Array(1,2,3)
    }
  }

  test("Distinct equal") {
    def distinctOp:(DataSetAPI[Int] => DataSetAPI[Int]) = ds => ds.distinct()

    assertDatasetOperationWithEqual(Seq(1,2,3), distinctOp) {
      case(r1,r2) => r1.collect().sorted sameElements r2.collect().sorted
    }
  }

}