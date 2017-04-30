package com.datawizards.sparklocal.rdd

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ZipTest extends SparkLocalBaseTest {

  test("Zip result") {
    assertRDDOperationResult(
      RDDAPI(Seq(1,2,3)) zip RDDAPI(Seq("a","b","c"))
    ) {
      Array((1,"a"),(2,"b"),(3,"c"))
    }
  }

  test("Zip equal") {
    val r1 = RDDAPI(Seq("a","b","c"))
    assertRDDOperation(Seq(1,2,3)){
      ds => ds zip r1
    }
  }

}