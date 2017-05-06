package com.datawizards.sparklocal.dataset

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RandomSplitTest extends SparkLocalBaseTest {

  val data:Range = 1 to 10

  test("randomSplit result - Scala") {
    val Array(sample1, sample2) = DataSetAPI(data).randomSplit(Array(80,20))
    assert(sample1.count() > sample2.count(), "First sample size > second sample size")
    assert(sample1.collect().forall(x => data contains x), "All sample elements from input data")
    assert(sample2.collect().forall(x => data contains x), "All sample elements from input data")
    assert(sample1.rdd().map(x => (x,1)).reduceByKey(_ + _).collectAsMap().values.forall(_ == 1), "All sample elements only once")
    assert(sample2.rdd().map(x => (x,1)).reduceByKey(_ + _).collectAsMap().values.forall(_ == 1), "All sample elements only once")
  }

  test("randomSplit result - Spark") {
    import spark.implicits._
    val Array(sample1, sample2) = DataSetAPI(spark.createDataset(data)).randomSplit(Array(80,20))
    assert(sample1.count() > sample2.count(), "First sample size > second sample size")
    assert(sample1.collect().forall(x => data contains x), "All sample elements from input data")
    assert(sample2.collect().forall(x => data contains x), "All sample elements from input data")
    assert(sample1.rdd().map(x => (x,1)).reduceByKey(_ + _).collectAsMap().values.forall(_ == 1), "All sample elements only once")
    assert(sample2.rdd().map(x => (x,1)).reduceByKey(_ + _).collectAsMap().values.forall(_ == 1), "All sample elements only once")
  }

  test("randomSplit result - negative weights") {
    intercept[IllegalArgumentException]{
      DataSetAPI(data).randomSplit(Array(-1,-2))
    }
  }

  test("randomSplit result - sum of weights is negative") {
    intercept[IllegalArgumentException]{
      DataSetAPI(data).randomSplit(Array(1,-2))
    }
  }

}