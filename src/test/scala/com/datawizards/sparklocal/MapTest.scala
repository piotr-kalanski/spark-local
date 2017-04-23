package com.datawizards.sparklocal

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MapTest extends SparkLocalBaseTest {

  test("Map values") {

    assertDatasetOperation(Seq(1,2,3)){
      ds => ds.map(x => x+1)
    }

  }

}