package com.datawizards.sparklocal.dataset

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.implicits._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SampleTest extends SparkLocalBaseTest {

  val data:Range = 1 to 10

  test("sample withoutReplacement result - Scala") {
    val sample = DataSetAPI(data).sample(withReplacement = false, 0.5, 0L)
    assert(sample.collect().forall(x => data contains x), "All sample elements from input data")
    assert(sample.rdd().map(x => (x,1)).reduceByKey(_ + _).collectAsMap().values.forall(_ == 1), "All sample elements only once")
  }

  test("sample withReplacement result - Scala") {
    val sample = DataSetAPI(data).sample(withReplacement = true, 2, 0L)
    assert(sample.collect().forall(x => data contains x), "All sample elements from input data")
    assert(sample.rdd().map(x => (x,1)).reduceByKey(_ + _).collectAsMap().values.exists(x => x > 1), "Some of sample elements appears twice or more")
  }

  test("sample withoutReplacement result - Spark") {
    import spark.implicits._
    val sample = DataSetAPI(spark.createDataset(data)).sample(withReplacement = false, 0.5, 0L)
    assert(sample.collect().forall(x => data contains x), "All sample elements from input data")
    assert(sample.rdd().map(x => (x,1)).reduceByKey(_ + _).collectAsMap().values.forall(_ == 1), "All sample elements only once")
  }

  test("sample withReplacement result - Spark") {
    import spark.implicits._
    val sample = DataSetAPI(spark.createDataset(data)).sample(withReplacement = true, 2, 0L)
    assert(sample.collect().forall(x => data contains x), "All sample elements from input data")
    assert(sample.rdd().map(x => (x,1)).reduceByKey(_ + _).collectAsMap().values.exists(x => x > 1), "Some of sample elements appears twice or more")
  }

}