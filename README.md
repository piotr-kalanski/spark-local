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
"com.github.piotr-kalanski" %% "spark-local" % "0.1.0"
```

or

```xml
<dependency>
    <groupId>com.github.piotr-kalanski</groupId>
    <artifactId>spark-local_2.11</artifactId>
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
|aggregate||
|cache| <ul><li>- [x] </li></ul>|
|cartesian||
|checkpoint||
|coalesce||
|collect| <ul><li>- [x] </li></ul>|
|count| <ul><li>- [x] </li></ul>|
|countApprox||
|countApproxDistinct||
|countByValue||
|countByValueApprox||
|dependencies||
|distinct||
|filter| <ul><li>- [x] </li></ul>|
|first||
|flatMap| <ul><li>- [x] </li></ul>|
|fold| <ul><li>- [x] </li></ul>|
|foreach| <ul><li>- [x] </li></ul>|
|foreachPartition| <ul><li>- [x] </li></ul>|
|getCheckpointFile||
|getNumPartitions||
|getStorageLevel||
|glom||
|groupBy||
|id||
|intersection| <ul><li>- [x] </li></ul>|
|isCheckpointed||
|isEmpty| <ul><li>- [x] </li></ul>|
|iterator||
|keyBy||
|localCheckpoint||
|map| <ul><li>- [x] </li></ul>|
|mapPartitions| <ul><li>- [x] </li></ul>|
|mapPartitionsWithIndex||
|max| <ul><li>- [x] </li></ul>|
|min| <ul><li>- [x] </li></ul>|
|name||
|partitioner||
|partitions||
|persist| <ul><li>- [x] </li></ul>|
|pipe||
|preferredLocations||
|randomSplit||
|reduce| <ul><li>- [x] </li></ul>|
|repartition||
|sample||
|saveAsObjectFile||
|saveAsTextFile||
|setName||
|sortBy| <ul><li>- [x] </li></ul>|
|subtract||
|take| <ul><li>- [x] </li></ul>|
|takeOrdered||
|takeSample||
|toDebugString||
|toJavaRDD||
|toLocalIterator||
|top||
|treeAggregate||
|treeReduce||
|union| <ul><li>- [x] </li></ul>|
|unpersist||
|zip| <ul><li>- [x] </li></ul>|
|zipPartitions||
|zipWithIndex| <ul><li>- [x] </li></ul>|
|zipWithUniqueId||

### Pair RDD API

|Operation|Supported?|
|---------|---------|
|aggregateByKey||
|cogroup||
|collectAsMap||
|combineByKey||
|combineByKeyWithClassTag||
|countApproxDistinctByKey||
|countByKey||
|countByKeyApprox||
|flatMapValues||
|foldByKey||
|fullOuterJoin||
|groupByKey||
|groupWith||
|join||
|keys||
|leftOuterJoin||
|lookup||
|mapValues||
|partitionBy||
|reduceByKey||
|reduceByKeyLocally||
|rightOuterJoin||
|sampleByKey||
|sampleByKeyExact||
|saveAsHadoopDataset||
|saveAsHadoopFile||
|saveAsNewAPIHadoopDataset||
|subtractByKey||
|values||

## Dataset API

### Dataset API - Actions

|Operation|Supported?|
|---------|---------|
|collect| <ul><li>- [x] </li></ul>|
|collectAsList||
|count| <ul><li>- [x] </li></ul>|
|describe||
|first| <ul><li>- [x] </li></ul>|
|foreach| <ul><li>- [x] </li></ul>|
|foreachPartition| <ul><li>- [x] </li></ul>|
|head| <ul><li>- [x] </li></ul>|
|reduce| <ul><li>- [x] </li></ul>|
|show||
|take| <ul><li>- [x] </li></ul>|
|takeAsList||
|toLocalIterator||

### Dataset API - Basic Dataset functions

|Operation|Supported?|
|---------|---------|
|as||
|cache| <ul><li>- [x] </li></ul>|
|checkpoint| <ul><li>- [x] </li></ul>|
|columns||
|createGlobalTempView||
|createOrReplaceTempView||
|createTempView||
|dtypes||
|explain||
|inputFiles||
|isLocal||
|javaRDD||
|persist| <ul><li>- [x] </li></ul>|
|printSchema||
|rdd||
|schema||
|storageLevel||
|toDF||
|toJavaRDD||
|unpersist||
|write||
|writeStream||

### Dataset API - streaming

|Operation|Supported?|
|---------|---------|
|isStreaming||
|withWatermark||

### Dataset API - Typed transformations

|Operation|Supported?|
|---------|---------|
|alias||
|as||
|coalesce||
|distinct||
|dropDuplicates||
|except||
|filter| <ul><li>- [x] </li></ul>|
|flatMap| <ul><li>- [x] </li></ul>|
|groupByKey||
|intersect||
|joinWith||
|limit||
|map| <ul><li>- [x] </li></ul>|
|mapPartitions| <ul><li>- [x] </li></ul>|
|orderBy||
|randomSplit||
|randomSplitAsList||
|repartition||
|sample||
|select||
|sort||
|sortWithinPartitions||
|transform||
|union||
|where||

### Dataset API - Untyped transformations

|Operation|Supported?|
|---------|---------|
|agg||
|apply||
|col||
|crossJoin||
|cube||
|drop||
|groupBy||
|join||
|na||
|rollup||
|select||
|selectExpr||
|stat||
|withColumn||
|withColumnRenamed||

# Bugs

Please report any bugs or submit feature requests to [spark-local Github issue tracker](https://github.com/piotr-kalanski/spark-local/issues).

# Continuous Integration

[Build History](https://travis-ci.org/piotr-kalanski/spark-local/builds)

# Contact

piotr.kalanski@gmail.com
