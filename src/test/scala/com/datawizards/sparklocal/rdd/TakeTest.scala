package com.datawizards.sparklocal.rdd

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TakeTest extends SparkLocalBaseTest {

  test("Take(n) result") {
    assert(RDDAPI(Seq(1,2,3)).take(2) sameElements Array(1,2))
  }

  test("TakeOrdered(n) result") {
    assert(RDDAPI(Seq(5,6,3,5,2,3,5,6,3,1)).takeOrdered(2) sameElements Array(1,2))
  }

  test("Take(n) equal") {
    def take2:(RDDAPI[Int] => Array[Int]) = rdd => rdd.take(2)

    assertRDDOperationReturnsSameResultWithEqual(Seq(1,2,3), take2) {
      case(r1,r2) => r1 sameElements r2
    }
  }

  test("TakeOrdered(n) equal") {
    def takeOrdered2:(RDDAPI[Int] => Array[Int]) = rdd => rdd.takeOrdered(2)

    assertRDDOperationReturnsSameResultWithEqual(Seq(5,6,3,5,2,3,5,6,3,1), takeOrdered2) {
      case(r1,r2) => r1 sameElements r2
    }
  }

}