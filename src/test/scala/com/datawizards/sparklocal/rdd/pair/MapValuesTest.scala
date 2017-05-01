package com.datawizards.sparklocal.rdd.pair

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.rdd.RDDAPI
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MapValuesTest extends SparkLocalBaseTest {

  test("Map values result") {
    assertRDDOperationResult(
      RDDAPI(Seq(("a",1),("b",2))).mapValues(x => x+1)
    ) {
      Array(("a",2),("b",3))
    }
  }

  test("Map values equal") {
    assertRDDOperationReturnsSameResult(Seq(("a",1),("b",2))){
      ds => ds.mapValues(x => x+1)
    }
  }

}