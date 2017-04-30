package com.datawizards.sparklocal.rdd

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class IntersectionTest extends SparkLocalBaseTest {

  test("Intersection result") {
    assertRDDOperationResult(
      RDDAPI(Seq(1,2,3,4)) intersection RDDAPI(Seq(5,4,3))
    ) {
      Array(3,4)
    }
  }

  test("Intersection equal") {
    val r2 = RDDAPI(Seq(3,4,5))
    assertRDDOperationWithEqual[Int, RDDAPI[Int]](Seq(1,2,3,4), ds => ds intersection r2) {
      case (d1,d2) => d1.collect().sorted sameElements d2.collect().sorted
    }
  }

}
