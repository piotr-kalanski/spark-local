package com.datawizards.sparklocal.rdd

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SortByTest extends SparkLocalBaseTest {

  test("sortBy result") {
    assertRDDOperationResult(
      RDDAPI(Seq((2,"b"),(3,"c"),(1,"a"))).sortBy(x=>x._1)
    ) {
      Array((1,"a"),(2,"b"),(3,"c"))
    }
  }

  test("sortBy equal") {
    assertRDDOperationReturnsSameResult(Seq((2,"b"),(3,"c"),(1,"a"))){
      ds => ds.sortBy(x=>x._1)
    }
  }

}