package com.datawizards.sparklocal.dataset

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.implicits._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ReduceTest extends SparkLocalBaseTest {

  test("Reduce result") {
      assert(DataSetAPI(Seq(1,2,3)).reduce(_ + _) == 6)
  }

  test("Reduce equal") {
    assertDatasetOperationReturnsSameResult(Seq(1,2,3)){
      ds => ds.reduce(_ + _)
    }
  }

}