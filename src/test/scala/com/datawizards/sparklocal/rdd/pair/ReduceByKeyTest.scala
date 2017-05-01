package com.datawizards.sparklocal.rdd.pair

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.rdd.RDDAPI
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ReduceByKeyTest extends SparkLocalBaseTest {

  val data = Seq(("a",1),("a",2),("b",2),("b",3),("c",1),("c",1),("c",2))

  test("reduceByKey result") {
    assertRDDOperationResultWithSorted(
      RDDAPI(data).reduceByKey(_ + _)
    ) {
      Array(("a",3),("b",5),("c",4))
    }
  }

  test("reduceByKey equal") {
    assertRDDOperationReturnsSameResultWithSorted(data){
      ds => ds.reduceByKey(_ + _)
    }
  }

}