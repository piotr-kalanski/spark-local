package com.datawizards.sparklocal.rdd

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CartesianTest extends SparkLocalBaseTest {

  test("Cartesian result") {
    assertRDDOperationResult(
      RDDAPI(Seq(1,2)) cartesian RDDAPI(Seq("a","b"))
    ) {
      Array((1,"a"),(1,"b"),(2,"a"),(2,"b"))
    }
  }

  test("Cartesian equal - scala cartesian Spark") {
    val r1 = RDDAPI(Seq("a","b","c"))
    assertRDDOperationReturnsSameResult(Seq(1,2,3)){
      ds => ds cartesian r1
    }
  }

  test("Cartesian equal - Spark cartesian Scala") {
    val r1 = RDDAPI(Seq("a","b","c"))
    assertRDDOperationReturnsSameResult(Seq(1,2,3)){
      ds => r1 cartesian ds
    }
  }

}