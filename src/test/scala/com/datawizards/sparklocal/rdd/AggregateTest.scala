package com.datawizards.sparklocal.rdd

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AggregateTest extends SparkLocalBaseTest {

  val data = Seq(("a",1),("b",2),("c",3))

  test("Aggregate result") {
      assert(RDDAPI(data).aggregate(1)({case (a,p) => a * p._2}, _ * _) == 6)
  }

  test("Aggregate equal") {
    assertRDDOperationReturnsSameResult(data){
      rdd => rdd.aggregate(1)({case (a,p) => a * p._2}, _ * _)
    }
  }

}