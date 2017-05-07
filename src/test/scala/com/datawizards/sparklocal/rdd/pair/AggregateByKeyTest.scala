package com.datawizards.sparklocal.rdd.pair

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.rdd.RDDAPI
import org.apache.spark.HashPartitioner
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AggregateByKeyTest extends SparkLocalBaseTest {

  val data = Seq(("a",1),("a",2),("b",2),("b",3),("c",1),("c",1),("c",2))

  test("aggregateByKey result") {
    assertRDDOperationResultWithSorted(
      RDDAPI(data).aggregateByKey(0)({case (a,p) => a + p}, _ + _)
    ) {
      Array(("a",3),("b",5),("c",4))
    }
  }

  test("aggregateByKey(numPartitions) result") {
    assertRDDOperationResultWithSorted(
      RDDAPI(data).aggregateByKey(0, 2)({case (a,p) => a + p},_ + _)
    ) {
      Array(("a",3),("b",5),("c",4))
    }
  }

  test("aggregateByKey(partitioner) result") {
    assertRDDOperationResultWithSorted(
      RDDAPI(data).aggregateByKey(0, new HashPartitioner(2))({case (a,p) => a + p},_ + _)
    ) {
      Array(("a",3),("b",5),("c",4))
    }
  }

  test("aggregateByKey equal") {
    assertRDDOperationReturnsSameResultWithSorted(data){
      ds => ds.aggregateByKey(0)({case (a,p) => a + p},_ + _)
    }
  }

  test("aggregateByKey(numPartitions) equal") {
    assertRDDOperationReturnsSameResultWithSorted(data){
      ds => ds.aggregateByKey(0, 1)({case (a,p) => a + p},_ + _)
    }
  }

  test("aggregateByKey(partitioner) equal") {
    assertRDDOperationReturnsSameResultWithSorted(data){
      ds => ds.aggregateByKey(0, new HashPartitioner(2))({case (a,p) => a + p},_ + _)
    }
  }

}