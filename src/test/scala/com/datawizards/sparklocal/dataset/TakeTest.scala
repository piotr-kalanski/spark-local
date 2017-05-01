package com.datawizards.sparklocal.dataset

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TakeTest extends SparkLocalBaseTest {

  test("Take(n) result") {
    assert(DataSetAPI(Seq(1,2,3)).take(2) sameElements Array(1,2))
  }


  test("Take(n) equal") {
    def take2:(DataSetAPI[Int] => Array[Int]) = ds => ds.take(2)

    assertDatasetOperationReturnsSameResultWithEqual(Seq(1,2,3), take2) {
      case(r1,r2) => r1 sameElements r2
    }
  }

}