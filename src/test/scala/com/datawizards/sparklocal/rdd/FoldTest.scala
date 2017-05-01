package com.datawizards.sparklocal.rdd

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FoldTest extends SparkLocalBaseTest {

  test("Fold result") {
      assert(RDDAPI(Seq(1,2,3)).fold(1)(_ * _) == 6)
  }

  test("Fold equal") {
    assertRDDOperationReturnsSameResult(Seq(1,2,3)){
      ds => ds.fold(0)(_ + _)
    }
  }

}