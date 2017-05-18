package com.datawizards.sparklocal.dataset

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.rdd.RDDAPI
import com.datawizards.sparklocal.implicits._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ToRDDTest extends SparkLocalBaseTest {

  val data = Seq(1,2,3)

  test("rdd - result") {
    assert(DataSetAPI(data).rdd() == RDDAPI(data))
  }

  test("rdd - equals") {
    assertDatasetOperationReturnsSameResult(data) {
      ds => ds.rdd()
    }
  }

}