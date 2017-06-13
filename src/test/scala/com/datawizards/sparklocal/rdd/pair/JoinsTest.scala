package com.datawizards.sparklocal.rdd.pair

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.rdd.RDDAPI
import org.apache.spark.HashPartitioner
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class JoinsTest extends SparkLocalBaseTest {

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

  test("join result") {
    assertRDDOperationResultWithSorted(leftRDD.join(rightRDD)) {
     Array(
       (1, ((2000,10), "Piotrek")),
       (1, ((2001,11), "Piotrek")),
       (2, ((2000,20), "Pawel")),
       (2, ((2001,21), "Pawel")),
       (2, ((2002,22), "Pawel"))
     )
    }
  }

  test("left join result") {
    assertRDDOperationResultWithSorted(leftRDD.leftOuterJoin(rightRDD)) {
      Array(
        (1, ((2000,10), Some("Piotrek"))),
        (1, ((2001,11), Some("Piotrek"))),
        (2, ((2000,20), Some("Pawel"))),
        (2, ((2001,21), Some("Pawel"))),
        (2, ((2002,22), Some("Pawel"))),
        (4, ((2000,40), None)),
        (4, ((2001,41), None))
      )
    }
  }

  test("right join result") {
    assertRDDOperationResultWithSorted(leftRDD.rightOuterJoin(rightRDD)) {
      Array(
        (1, (Some((2000,10)), "Piotrek")),
        (1, (Some((2001,11)), "Piotrek")),
        (2, (Some((2000,20)), "Pawel")),
        (2, (Some((2001,21)), "Pawel")),
        (2, (Some((2002,22)), "Pawel")),
        (3, (None, "Michal"))
      )
    }
  }

  test("full outer join result") {
    assertRDDOperationResultWithSorted(leftRDD.fullOuterJoin(rightRDD)) {
      Array(
        (1, (Some((2000,10)), Some("Piotrek"))),
        (1, (Some((2001,11)), Some("Piotrek"))),
        (2, (Some((2000,20)), Some("Pawel"))),
        (2, (Some((2001,21)), Some("Pawel"))),
        (2, (Some((2002,22)), Some("Pawel"))),
        (4, (Some((2000,40)), None)),
        (4, (Some((2001,41)), None)),
        (3, (None, Some("Michal")))
      )
    }
  }

  test("join - Scala, Spark - equals") {
    assertRDDOperationReturnsSameResultWithSorted(left){
      rdd => rdd.join(rightRDD)
    }
    assertRDDOperationReturnsSameResultWithSorted(left){
      rdd => rdd.join(rightRDD,2)
    }
    assertRDDOperationReturnsSameResultWithSorted(left){
      rdd => rdd.join(rightRDD,new HashPartitioner(2))
    }
  }

  test("left join - Scala, Spark  - equals") {
    assertRDDOperationReturnsSameResultWithSorted(left){
      rdd => rdd.leftOuterJoin(rightRDD)
    }
    assertRDDOperationReturnsSameResultWithSorted(left){
      rdd => rdd.leftOuterJoin(rightRDD,2)
    }
    assertRDDOperationReturnsSameResultWithSorted(left){
      rdd => rdd.leftOuterJoin(rightRDD,new HashPartitioner(2))
    }
  }

  test("right join - Scala, Spark  - equals") {
    assertRDDOperationReturnsSameResultWithSorted(left){
      rdd => rdd.rightOuterJoin(rightRDD)
    }
    assertRDDOperationReturnsSameResultWithSorted(left){
      rdd => rdd.rightOuterJoin(rightRDD,2)
    }
    assertRDDOperationReturnsSameResultWithSorted(left){
      rdd => rdd.rightOuterJoin(rightRDD,new HashPartitioner(2))
    }
  }

  test("full outer join - Scala, Spark  - equals") {
    assertRDDOperationReturnsSameResultWithSorted(left){
      rdd => rdd.fullOuterJoin(rightRDD)
    }
    assertRDDOperationReturnsSameResultWithSorted(left){
      rdd => rdd.fullOuterJoin(rightRDD,2)
    }
    assertRDDOperationReturnsSameResultWithSorted(left){
      rdd => rdd.fullOuterJoin(rightRDD,new HashPartitioner(2))
    }
  }

  test("join - Spark, Scala - equals") {
    assertRDDOperationReturnsSameResultWithSorted(left){
      rdd => rightRDD.join(rdd)
    }
    assertRDDOperationReturnsSameResultWithSorted(left){
      rdd => rightRDD.join(rdd,2)
    }
    assertRDDOperationReturnsSameResultWithSorted(left){
      rdd => rightRDD.join(rdd,new HashPartitioner(2))
    }
  }

  test("left join - Spark, Scala  - equals") {
    assertRDDOperationReturnsSameResultWithSorted(left){
      rdd => rightRDD.leftOuterJoin(rdd)
    }
    assertRDDOperationReturnsSameResultWithSorted(left){
      rdd => rightRDD.leftOuterJoin(rdd,2)
    }
    assertRDDOperationReturnsSameResultWithSorted(left){
      rdd => rightRDD.leftOuterJoin(rdd,new HashPartitioner(2))
    }
  }

  test("right join - Spark, Scala  - equals") {
    assertRDDOperationReturnsSameResultWithSorted(left){
      rdd => rightRDD.rightOuterJoin(rdd)
    }
    assertRDDOperationReturnsSameResultWithSorted(left){
      rdd => rightRDD.rightOuterJoin(rdd,2)
    }
    assertRDDOperationReturnsSameResultWithSorted(left){
      rdd => rightRDD.rightOuterJoin(rdd,new HashPartitioner(2))
    }
  }

  test("full outer join - Spark, Scala  - equals") {
    assertRDDOperationReturnsSameResultWithSorted(left){
      rdd => rightRDD.fullOuterJoin(rdd)
    }
    assertRDDOperationReturnsSameResultWithSorted(left){
      rdd => rightRDD.fullOuterJoin(rdd,2)
    }
    assertRDDOperationReturnsSameResultWithSorted(left){
      rdd => rightRDD.fullOuterJoin(rdd,new HashPartitioner(2))
    }
  }
}