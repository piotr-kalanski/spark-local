package com.datawizards.sparklocal.dataset

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class IntersectionTest extends SparkLocalBaseTest {

  val r2 = DataSetAPI(Seq(3,4,5))

  test("Intersection result") {
    assertDatasetOperationResult(
      DataSetAPI(Seq(1,2,3,4)) intersect DataSetAPI(Seq(5,4,3))
    ) {
      Array(3,4)
    }
  }

  test("Intersection equal - scala intersection spark") {
    assertDatasetOperationReturnsSameResultWithSorted(Seq(1,2,3,4)){
      ds => ds intersect r2
    }
  }

  test("Intersection equal - spark intersection scala") {
    assertDatasetOperationReturnsSameResultWithSorted(Seq(1,2,3,4)) {
      ds => r2 intersect ds
    }
  }

}
