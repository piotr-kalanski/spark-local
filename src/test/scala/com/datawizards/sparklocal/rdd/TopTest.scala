package com.datawizards.sparklocal.rdd

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TopTest extends SparkLocalBaseTest {

  test("Top result") {
    assert(RDDAPI(Seq(6,4,7,4,6,3,4,6,9,2,3,1,8)).top(2) sameElements Array(9,8))
  }

  test("Top equal") {
    def top2:(RDDAPI[Int] => Array[Int]) = ds => ds.top(2)

    assertRDDOperationWithEqual(Seq(6,4,7,4,6,3,4,6,9,2,3,1,8), top2) {
      case(r1,r2) => r1 sameElements r2
    }
  }

}