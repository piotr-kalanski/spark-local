package com.datawizards.sparklocal.rdd.pair

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.rdd.RDDAPI
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FlatMapValuesTest extends SparkLocalBaseTest {

  test("flatMapValues result") {
    assertRDDOperationResult(
      RDDAPI(Seq(("a",1),("b",2))).flatMapValues(x => 1 to x)
    ) {
      Array(("a",1),("b",1),("b",2))
    }
  }

  test("flatMapValues equal") {
    assertRDDOperation(Seq(("a",1),("b",2))){
      ds => ds.flatMapValues(x => 1 to x)
    }
  }

}