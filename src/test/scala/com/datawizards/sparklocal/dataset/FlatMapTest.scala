package com.datawizards.sparklocal.dataset

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.implicits._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FlatMapTest extends SparkLocalBaseTest {

  test("Map result") {
    assertDatasetOperationResult(
      DataSetAPI(Seq(1,2,3)).flatMap(x => 1 to x)
    ) {
      Array(1,1,2,1,2,3)
    }
  }

  test("Map equal") {
    assertDatasetOperationReturnsSameResult(Seq(1,2,3)){
      ds => ds.flatMap(x => 1 until x)
    }
  }

}