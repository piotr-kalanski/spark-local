package com.datawizards.sparklocal.rdd

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ZipWithIndexTest extends SparkLocalBaseTest {

  test("Zip with index result") {
    assertRDDOperationResult(
      RDDAPI(Seq("a","b","c")).zipWithIndex()
    ) {
      Array(("a",0),("b",1),("c",2))
    }
  }

  test("Zip with index equal") {
    assertRDDOperationReturnsSameResult(Seq("a","b","c")){
      ds => ds.zipWithIndex()
    }
  }

}