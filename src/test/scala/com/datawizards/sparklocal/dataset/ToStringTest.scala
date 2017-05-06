package com.datawizards.sparklocal.dataset

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ToStringTest extends SparkLocalBaseTest {

  test("toString result") {
      assert(DataSetAPI(Seq(1,2,3)).toString == "DataSet(1,2,3)")
  }

  test("toString equal") {
    assertDatasetOperationReturnsSameResult(Seq(1,2,3)){
      ds => ds.toString
    }
  }

}