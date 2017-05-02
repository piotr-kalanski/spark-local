package com.datawizards.sparklocal.rdd

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HeadTest extends SparkLocalBaseTest {

  test("First result") {
    assert(RDDAPI(Seq(1,2,3)).first() == 1)
  }

  test("Head result") {
      assert(RDDAPI(Seq(1,2,3)).head() == 1)
  }

  test("Head(n) result") {
    assert(RDDAPI(Seq(1,2,3)).head(2) sameElements Array(1,2))
  }

  test("Head equal") {
    assertRDDOperationReturnsSameResult(Seq(1,2,3)){
      ds => ds.head()
    }
  }

  test("First equal") {
    assertRDDOperationReturnsSameResult(Seq(1,2,3)){
      ds => ds.first()
    }
  }

  test("Head(n) equal") {
    def head2:(RDDAPI[Int] => Array[Int]) = ds => ds.head(2)

    assertRDDOperationReturnsSameResultWithEqual(Seq(1,2,3), head2) {
      case(r1,r2) => r1 sameElements r2
    }
  }


}