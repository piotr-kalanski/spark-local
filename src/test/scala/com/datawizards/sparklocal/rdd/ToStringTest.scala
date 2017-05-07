package com.datawizards.sparklocal.rdd

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ToStringTest extends SparkLocalBaseTest {

  test("ToString result") {
      val rdd = RDDAPI(Seq(1,2,3))
      assert(rdd.toString() == "RDD(1,2,3)")
  }

}