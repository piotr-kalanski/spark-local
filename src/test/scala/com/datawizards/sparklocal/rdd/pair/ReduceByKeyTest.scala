package com.datawizards.sparklocal.rdd.pair

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.rdd.RDDAPI
import org.apache.spark.HashPartitioner
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

  test("reduceByKeyLocally result") {
    assert(RDDAPI(data).reduceByKeyLocally(_ + _) == Map(("a",3),("b",5),("c",4)))
  }

  test("reduceByKey(numPartitions) result") {
    assertRDDOperationResultWithSorted(
      RDDAPI(data).reduceByKey(_ + _, 2)
    ) {
      Array(("a",3),("b",5),("c",4))
    }
  }

  test("reduceByKey(partitioner) result") {
    assertRDDOperationResultWithSorted(
      RDDAPI(data).reduceByKey(new HashPartitioner(2), _ + _)
    ) {
      Array(("a",3),("b",5),("c",4))
    }
  }

  test("reduceByKey equal") {
    assertRDDOperationReturnsSameResultWithSorted(data){
      ds => ds.reduceByKey(_ + _)
    }
  }

  test("reduceByKey(numPartitions) equal") {
    assertRDDOperationReturnsSameResultWithSorted(data){
      ds => ds.reduceByKey(_ + _, 1)
    }
  }

  test("reduceByKey(partitioner) equal") {
    assertRDDOperationReturnsSameResultWithSorted(data){
      ds => ds.reduceByKey(new HashPartitioner(2), _ + _)
    }
  }

  test("reduceByKeyLocally equal") {
    assertRDDOperationReturnsSameResult(data){
      ds => ds.reduceByKeyLocally(_ + _)
    }
  }

}