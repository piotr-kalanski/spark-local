package com.datawizards.sparklocal

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MapTest extends SparkLocalBaseTest {

  test("Map result") {
    assertDatasetOperationResult(
      DataSetAPI(Seq(1,2,3)).map(x => x+1)
    ) {
      Array(2,3,4)
    }
  }

  test("Map equal") {
    assertDatasetOperation(Seq(1,2,3)){
      ds => ds.map(x => x+1)
    }
  }

}