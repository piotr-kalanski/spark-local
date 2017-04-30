package com.datawizards.sparklocal.rdd

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TakeTest extends SparkLocalBaseTest {

  test("Take(n) result") {
    assert(RDDAPI(Seq(1,2,3)).take(2) sameElements Array(1,2))
  }


  test("Take(n) equal") {
    def take2:(RDDAPI[Int] => Array[Int]) = ds => ds.take(2)

    assertRDDOperationWithEqual(Seq(1,2,3), take2) {
      case(r1,r2) => r1 sameElements r2
    }
  }

}