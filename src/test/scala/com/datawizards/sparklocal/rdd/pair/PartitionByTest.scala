package com.datawizards.sparklocal.rdd.pair

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.rdd.RDDAPI
import org.apache.spark.HashPartitioner
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PartitionByTest extends SparkLocalBaseTest {

  test("PartitionBy result") {
      val rdd = RDDAPI(Seq(("a",1),("b",2)))
      assert(rdd.partitionBy(new HashPartitioner(2)) == rdd)
  }

  test("PartitionBy equal") {
    assertRDDOperationReturnsSameResult(Seq(("a",1),("b",2))){
      rdd => rdd.partitionBy(new HashPartitioner(2)).collectAsMap()
    }
  }

}