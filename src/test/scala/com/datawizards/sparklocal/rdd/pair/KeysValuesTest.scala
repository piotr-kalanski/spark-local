package com.datawizards.sparklocal.rdd.pair

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.rdd.RDDAPI
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class KeysValuesTest extends SparkLocalBaseTest {

  test("Keys result") {
    assertRDDOperationResult(
      RDDAPI(Seq(("a",1),("b",2))).keys
    ) {
      Array("a","b")
    }
  }

  test("Values result") {
    assertRDDOperationResult(
      RDDAPI(Seq(("a",1),("b",2))).values
    ) {
      Array(1,2)
    }
  }

  test("Keys equal") {
    assertRDDOperationReturnsSameResult(Seq(("a",1),("b",2))){
      ds => ds.keys
    }
  }

  test("Values equal") {
    assertRDDOperationReturnsSameResult(Seq(("a",1),("b",2))){
      ds => ds.values
    }
  }

}