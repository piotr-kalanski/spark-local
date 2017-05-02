package com.datawizards.sparklocal.dataset

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HeadTest extends SparkLocalBaseTest {

  test("Head result") {
      assert(DataSetAPI(Seq(1,2,3)).head() == 1)
  }

  test("Head(n) result") {
    assert(DataSetAPI(Seq(1,2,3)).head(2) sameElements Array(1,2))
  }


  test("Head equal") {
    assertDatasetOperationReturnsSameResult(Seq(1,2,3)){
      ds => ds.head()
    }
  }

  test("Head(n) equal") {
    def head2:(DataSetAPI[Int] => Array[Int]) = ds => ds.head(2)

    assertDatasetOperationReturnsSameResultWithEqual(Seq(1,2,3), head2) {
      case(r1,r2) => r1 sameElements r2
    }
  }


}