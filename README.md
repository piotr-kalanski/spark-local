# spark-local

API enabling switching between Spark execution engine and local fast implementation based on Scala collections.

[![Build Status](https://api.travis-ci.org/piotr-kalanski/spark-local.png?branch=development)](https://api.travis-ci.org/piotr-kalanski/spark-local.png?branch=development)
[![codecov.io](http://codecov.io/github/piotr-kalanski/spark-local/coverage.svg?branch=development)](http://codecov.io/github/piotr-kalanski/spark-local/coverage.svg?branch=development)
[<img src="https://img.shields.io/maven-central/v/com.github.piotr-kalanski/spark-local.svg?label=latest%20release"/>](http://search.maven.org/#search|ga|1|a%3A%22spark-local%22)
[![License](http://img.shields.io/:license-Apache%202-red.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)

# Table of contents

- [Goals](#goals)
- [Getting started](#getting-started)
- [Examples](#examples)
- [Supported operations](#supported-operations)

# Goals

- Speed up unit testing when using Spark
- Enable possibility to switch between Spark execution engine and Scala collections depending on use case, especially size of data

# Getting started

Include dependency:

```scala
"com.github.piotr-kalanski" % "spark-local" % "0.1.0"
```

or

```xml
<dependency>
    <groupId>com.github.piotr-kalanski</groupId>
    <artifactId>spark-local</artifactId>
    <version>0.1.0</version>
</dependency>
```

# Examples

## Example - Dataset API

```scala
import com.datawizards.sparklocal.dataset.DataSetAPI
import org.apache.spark.sql.SparkSession

object ExampleDataset1 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._

    val data = Seq(1,2,3)
    val ds = data.toDS()

    assertEquals(
      calculateSum(DataSetAPI(data)),
      calculateSum(DataSetAPI(ds))
    )

    assertEquals(
      calculateSumOfSquares(DataSetAPI(data)),
      calculateSumOfSquares(DataSetAPI(ds))
    )

  }

  def assertEquals[T](r1:T, r2:T): Unit = {
    println(r1)
    assert(r1 == r2)
  }

  def calculateSum(ds: DataSetAPI[Int]): Int = ds.reduce(_ + _)
  def calculateSumOfSquares(ds: DataSetAPI[Int]): Int = ds.map(x=>x*x).reduce(_ + _)

}
```

## Example - RDD API

```scala
import com.datawizards.sparklocal.rdd.RDDAPI
import org.apache.spark.sql.SparkSession

object ExampleRDD1 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()

    val data = Seq(1,2,3)
    val rdd = spark.sparkContext.parallelize(data)

    assertEquals(
      calculateSum(RDDAPI(data)),
      calculateSum(RDDAPI(rdd))
    )

    assertEquals(
      calculateSumOfSquares(RDDAPI(data)),
      calculateSumOfSquares(RDDAPI(rdd))
    )

  }

  def assertEquals[T](r1:T, r2:T): Unit = {
    println(r1)
    assert(r1 == r2)
  }

  def calculateSum(ds: RDDAPI[Int]): Int = ds.reduce(_ + _)
  def calculateSumOfSquares(ds: RDDAPI[Int]): Int = ds.map(x=>x*x).reduce(_ + _)

}
```

# Supported operations

## RDD API

### RDD API - basic actions and transformations

|Operation|Supported?|
|---------|---------|
|aggregate| <ul><li>- [] </li></ul>|
|cache| <ul><li>- [x] </li></ul>|
|cartesian| <ul><li>- [] </li></ul>|
|checkpoint| <ul><li>- [] </li></ul>|
|coalesce| <ul><li>- [] </li></ul>|
|collect| <ul><li>- [x] </li></ul>|
|count| <ul><li>- [x] </li></ul>|
|countApprox| <ul><li>- [] </li></ul>|
|countApproxDistinct| <ul><li>- [] </li></ul>|
|countByValue| <ul><li>- [] </li></ul>|
|countByValueApprox| <ul><li>- [] </li></ul>|
|dependencies| <ul><li>- [] </li></ul>|
|distinct| <ul><li>- [] </li></ul>|
|filter| <ul><li>- [x] </li></ul>|
|first| <ul><li>- [] </li></ul>|
|flatMap| <ul><li>- [x] </li></ul>|
|fold| <ul><li>- [x] </li></ul>|
|foreach| <ul><li>- [x] </li></ul>|
|foreachPartition| <ul><li>- [x] </li></ul>|
|getCheckpointFile| <ul><li>- [] </li></ul>|
|getNumPartitions| <ul><li>- [] </li></ul>|
|getStorageLevel| <ul><li>- [] </li></ul>|
|glom| <ul><li>- [] </li></ul>|
|groupBy| <ul><li>- [] </li></ul>|
|id| <ul><li>- [] </li></ul>|
|intersection| <ul><li>- [x] </li></ul>|
|isCheckpointed| <ul><li>- [] </li></ul>|
|isEmpty| <ul><li>- [x] </li></ul>|
|iterator| <ul><li>- [] </li></ul>|
|keyBy| <ul><li>- [] </li></ul>|
|localCheckpoint| <ul><li>- [] </li></ul>|
|map| <ul><li>- [x] </li></ul>|
|mapPartitions| <ul><li>- [x] </li></ul>|
|mapPartitionsWithIndex| <ul><li>- [] </li></ul>|
|max| <ul><li>- [x] </li></ul>|
|min| <ul><li>- [x] </li></ul>|
|name| <ul><li>- [] </li></ul>|
|partitioner| <ul><li>- [] </li></ul>|
|partitions| <ul><li>- [] </li></ul>|
|persist| <ul><li>- [x] </li></ul>|
|pipe| <ul><li>- [] </li></ul>|
|preferredLocations| <ul><li>- [] </li></ul>|
|randomSplit| <ul><li>- [] </li></ul>|
|reduce| <ul><li>- [x] </li></ul>|
|repartition| <ul><li>- [] </li></ul>|
|sample| <ul><li>- [] </li></ul>|
|saveAsObjectFile| <ul><li>- [] </li></ul>|
|saveAsTextFile| <ul><li>- [] </li></ul>|
|setName| <ul><li>- [] </li></ul>|
|sortBy| <ul><li>- [x] </li></ul>|
|subtract| <ul><li>- [] </li></ul>|
|take| <ul><li>- [x] </li></ul>|
|takeOrdered| <ul><li>- [] </li></ul>|
|takeSample| <ul><li>- [] </li></ul>|
|toDebugString| <ul><li>- [] </li></ul>|
|toJavaRDD| <ul><li>- [] </li></ul>|
|toLocalIterator| <ul><li>- [] </li></ul>|
|top| <ul><li>- [] </li></ul>|
|treeAggregate| <ul><li>- [] </li></ul>|
|treeReduce| <ul><li>- [] </li></ul>|
|union| <ul><li>- [x] </li></ul>|
|unpersist| <ul><li>- [] </li></ul>|
|zip| <ul><li>- [x] </li></ul>|
|zipPartitions| <ul><li>- [] </li></ul>|
|zipWithIndex| <ul><li>- [x] </li></ul>|
|zipWithUniqueId| <ul><li>- [] </li></ul>|

### Pair RDD API

|Operation|Supported?|
|---------|---------|
|aggregateByKey| <ul><li>- [] </li></ul>|
|cogroup| <ul><li>- [] </li></ul>|
|collectAsMap| <ul><li>- [] </li></ul>|
|combineByKey| <ul><li>- [] </li></ul>|
|combineByKeyWithClassTag| <ul><li>- [] </li></ul>|
|countApproxDistinctByKey| <ul><li>- [] </li></ul>|
|countByKey| <ul><li>- [] </li></ul>|
|countByKeyApprox| <ul><li>- [] </li></ul>|
|flatMapValues| <ul><li>- [] </li></ul>|
|foldByKey| <ul><li>- [] </li></ul>|
|fullOuterJoin| <ul><li>- [] </li></ul>|
|groupByKey| <ul><li>- [] </li></ul>|
|groupWith| <ul><li>- [] </li></ul>|
|join| <ul><li>- [] </li></ul>|
|keys| <ul><li>- [] </li></ul>|
|leftOuterJoin| <ul><li>- [] </li></ul>|
|lookup| <ul><li>- [] </li></ul>|
|mapValues| <ul><li>- [] </li></ul>|
|partitionBy| <ul><li>- [] </li></ul>|
|reduceByKey| <ul><li>- [] </li></ul>|
|reduceByKeyLocally| <ul><li>- [] </li></ul>|
|rightOuterJoin| <ul><li>- [] </li></ul>|
|sampleByKey| <ul><li>- [] </li></ul>|
|sampleByKeyExact| <ul><li>- [] </li></ul>|
|saveAsHadoopDataset| <ul><li>- [] </li></ul>|
|saveAsHadoopFile| <ul><li>- [] </li></ul>|
|saveAsNewAPIHadoopDataset| <ul><li>- [] </li></ul>|
|subtractByKey| <ul><li>- [] </li></ul>|
|values| <ul><li>- [] </li></ul>|

## Dataset API

### Dataset API - Actions

|Operation|Supported?|
|---------|---------|
|collect| <ul><li>- [x] </li></ul>|
|collectAsList| <ul><li>- [] </li></ul>|
|count| <ul><li>- [x] </li></ul>|
|describe| <ul><li>- [] </li></ul>|
|first| <ul><li>- [x] </li></ul>|
|foreach| <ul><li>- [x] </li></ul>|
|foreachPartition| <ul><li>- [x] </li></ul>|
|head| <ul><li>- [x] </li></ul>|
|reduce| <ul><li>- [x] </li></ul>|
|show| <ul><li>- [] </li></ul>|
|take| <ul><li>- [x] </li></ul>|
|takeAsList| <ul><li>- [] </li></ul>|
|toLocalIterator| <ul><li>- [] </li></ul>|

### Dataset API - Basic Dataset functions

|Operation|Supported?|
|---------|---------|
|as| <ul><li>- [] </li></ul>|
|cache| <ul><li>- [x] </li></ul>|
|checkpoint| <ul><li>- [x] </li></ul>|
|columns| <ul><li>- [] </li></ul>|
|createGlobalTempView| <ul><li>- [] </li></ul>|
|createOrReplaceTempView| <ul><li>- [] </li></ul>|
|createTempView| <ul><li>- [] </li></ul>|
|dtypes| <ul><li>- [] </li></ul>|
|explain| <ul><li>- [] </li></ul>|
|inputFiles| <ul><li>- [] </li></ul>|
|isLocal| <ul><li>- [] </li></ul>|
|javaRDD| <ul><li>- [] </li></ul>|
|persist| <ul><li>- [x] </li></ul>|
|printSchema| <ul><li>- [] </li></ul>|
|rdd| <ul><li>- [] </li></ul>|
|schema| <ul><li>- [] </li></ul>|
|storageLevel| <ul><li>- [] </li></ul>|
|toDF| <ul><li>- [] </li></ul>|
|toJavaRDD| <ul><li>- [] </li></ul>|
|unpersist| <ul><li>- [] </li></ul>|
|write| <ul><li>- [] </li></ul>|
|writeStream| <ul><li>- [] </li></ul>|

### Dataset API - streaming

|Operation|Supported?|
|---------|---------|
|isStreaming| <ul><li>- [] </li></ul>|
|withWatermark| <ul><li>- [] </li></ul>|

### Dataset API - Typed transformations

|Operation|Supported?|
|---------|---------|
|alias| <ul><li>- [] </li></ul>|
|as| <ul><li>- [] </li></ul>|
|coalesce| <ul><li>- [] </li></ul>|
|distinct| <ul><li>- [] </li></ul>|
|dropDuplicates| <ul><li>- [] </li></ul>|
|except| <ul><li>- [] </li></ul>|
|filter| <ul><li>- [x] </li></ul>|
|flatMap| <ul><li>- [x] </li></ul>|
|groupByKey| <ul><li>- [] </li></ul>|
|intersect| <ul><li>- [] </li></ul>|
|joinWith| <ul><li>- [] </li></ul>|
|limit| <ul><li>- [] </li></ul>|
|map| <ul><li>- [x] </li></ul>|
|mapPartitions| <ul><li>- [x] </li></ul>|
|orderBy| <ul><li>- [] </li></ul>|
|randomSplit| <ul><li>- [] </li></ul>|
|randomSplitAsList| <ul><li>- [] </li></ul>|
|repartition| <ul><li>- [] </li></ul>|
|sample| <ul><li>- [] </li></ul>|
|select| <ul><li>- [] </li></ul>|
|sort| <ul><li>- [] </li></ul>|
|sortWithinPartitions| <ul><li>- [] </li></ul>|
|transform| <ul><li>- [] </li></ul>|
|union| <ul><li>- [] </li></ul>|
|where| <ul><li>- [] </li></ul>|

### Dataset API - Untyped transformations

|Operation|Supported?|
|---------|---------|
|agg| <ul><li>- [] </li></ul>|
|apply| <ul><li>- [] </li></ul>|
|col| <ul><li>- [] </li></ul>|
|crossJoin| <ul><li>- [] </li></ul>|
|cube| <ul><li>- [] </li></ul>|
|drop| <ul><li>- [] </li></ul>|
|groupBy| <ul><li>- [] </li></ul>|
|join| <ul><li>- [] </li></ul>|
|na| <ul><li>- [] </li></ul>|
|rollup| <ul><li>- [] </li></ul>|
|select| <ul><li>- [] </li></ul>|
|selectExpr| <ul><li>- [] </li></ul>|
|stat| <ul><li>- [] </li></ul>|
|withColumn| <ul><li>- [] </li></ul>|
|withColumnRenamed| <ul><li>- [] </li></ul>|

# Bugs

Please report any bugs or submit feature requests to [spark-local Github issue tracker](https://github.com/piotr-kalanski/spark-local/issues).

# Continuous Integration

[Build History](https://travis-ci.org/piotr-kalanski/spark-local/builds)

# Contact

piotr.kalanski@gmail.com
