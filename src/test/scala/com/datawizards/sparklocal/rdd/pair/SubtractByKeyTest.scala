package com.datawizards.sparklocal.rdd.pair

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.rdd.RDDAPI
import org.apache.spark.HashPartitioner
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SubtractByKeyTest extends SparkLocalBaseTest {

  val left = Seq(
    (1, (2000,10)),
    (1, (2001,11)),
    (2, (2000,20)),
    (2, (2001,21)),
    (2, (2002,22)),
    (4, (2000,40)),
    (4, (2001,41))
  )
  val right = Seq(
    (1, "Piotrek"),
    (2, "Pawel"),
    (3, "Michal")
  )
  val leftRDD = RDDAPI(left)
  val rightRDD = RDDAPI(right)

  test("subtractByKey result") {
    assertRDDOperationResult(leftRDD.subtractByKey(rightRDD)) {
     Array(
       (4, (2000,40)),
       (4, (2001,41))
     )
    }
  }

  test("subtractByKey - Scala, Spark - equals") {
    assertRDDOperationReturnsSameResultWithSorted(left){
      rdd => rdd.subtractByKey(rightRDD)
    }
    assertRDDOperationReturnsSameResultWithSorted(left){
      rdd => rdd.subtractByKey(rightRDD,2)
    }
    assertRDDOperationReturnsSameResultWithSorted(left){
      rdd => rdd.subtractByKey(rightRDD,new HashPartitioner(2))
    }
  }

  test("subtractByKey - Spark, Scala - equals") {
    assertRDDOperationReturnsSameResultWithSorted(left){
      rdd => rightRDD.subtractByKey(rdd)
    }
    assertRDDOperationReturnsSameResultWithSorted(left){
      rdd => rightRDD.subtractByKey(rdd,2)
    }
    assertRDDOperationReturnsSameResultWithSorted(left){
      rdd => rightRDD.subtractByKey(rdd,new HashPartitioner(2))
    }
  }

}