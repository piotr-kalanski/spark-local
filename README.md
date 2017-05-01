# spark-local

API enabling switching between Spark execution engine and local implementation based on Scala collections.

[![Build Status](https://api.travis-ci.org/piotr-kalanski/spark-local.png?branch=development)](https://api.travis-ci.org/piotr-kalanski/spark-local.png?branch=development)
[![codecov.io](http://codecov.io/github/piotr-kalanski/spark-local/coverage.svg?branch=development)](http://codecov.io/github/piotr-kalanski/spark-local/coverage.svg?branch=development)
[<img src="https://img.shields.io/maven-central/v/com.github.piotr-kalanski/spark-local_2.11.svg?label=latest%20release"/>](http://search.maven.org/#search|ga|1|a%3A%22spark-local%22)
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
|cache|![](images/API-supported-green.png)|
|cartesian||
|checkpoint|![](images/API-supported-green.png)|
|coalesce||
|collect| ![](images/API-supported-green.png)|
|count| ![](images/API-supported-green.png)|
|countApprox||
|countApproxDistinct||
|countByValue||
|countByValueApprox||
|dependencies||
|distinct|![](images/API-supported-green.png)|
|filter| ![](images/API-supported-green.png)|
|first||
|flatMap| ![](images/API-supported-green.png)|
|fold| ![](images/API-supported-green.png)|
|foreach| ![](images/API-supported-green.png)|
|foreachPartition| ![](images/API-supported-green.png)|
|getCheckpointFile||
|getNumPartitions||
|getStorageLevel||
|glom||
|groupBy||
|id||
|intersection| ![](images/API-supported-green.png)|
|isCheckpointed||
|isEmpty| ![](images/API-supported-green.png)|
|iterator||
|keyBy||
|localCheckpoint||
|map| ![](images/API-supported-green.png)|
|mapPartitions| ![](images/API-supported-green.png)|
|mapPartitionsWithIndex||
|max| ![](images/API-supported-green.png)|
|min| ![](images/API-supported-green.png)|
|name||
|partitioner||
|partitions||
|persist| ![](images/API-supported-green.png)|
|pipe||
|preferredLocations||
|randomSplit||
|reduce| ![](images/API-supported-green.png)|
|repartition||
|sample||
|saveAsObjectFile||
|saveAsTextFile||
|setName||
|sortBy| ![](images/API-supported-green.png)|
|subtract||
|take| ![](images/API-supported-green.png)|
|takeOrdered||
|takeSample||
|toDebugString||
|toJavaRDD||
|toLocalIterator||
|top|![](images/API-supported-green.png)|
|treeAggregate||
|treeReduce||
|union| ![](images/API-supported-green.png)|
|unpersist||
|zip| ![](images/API-supported-green.png)|
|zipPartitions||
|zipWithIndex| ![](images/API-supported-green.png)|
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
|countByKey|![](images/API-supported-green.png)|
|countByKeyApprox||
|flatMapValues|![](images/API-supported-green.png)|
|foldByKey|![](images/API-supported-green.png)|
|fullOuterJoin|![](images/API-supported-green.png)|
|groupByKey|![](images/API-supported-green.png)|
|groupWith||
|join|![](images/API-supported-green.png)|
|keys|![](images/API-supported-green.png)|
|leftOuterJoin|![](images/API-supported-green.png)|
|lookup||
|mapValues|![](images/API-supported-green.png)|
|partitionBy||
|reduceByKey|![](images/API-supported-green.png)|
|reduceByKeyLocally|![](images/API-supported-green.png)|
|rightOuterJoin|![](images/API-supported-green.png)|
|sampleByKey||
|sampleByKeyExact||
|saveAsHadoopDataset||
|saveAsHadoopFile||
|saveAsNewAPIHadoopDataset||
|subtractByKey||
|values|![](images/API-supported-green.png)|

## Dataset API

### Dataset API - Actions

|Operation|Supported?|
|---------|---------|
|collect| ![](images/API-supported-green.png)|
|collectAsList|![](images/API-supported-green.png)|
|count| ![](images/API-supported-green.png)|
|describe||
|first| ![](images/API-supported-green.png)|
|foreach| ![](images/API-supported-green.png)|
|foreachPartition| ![](images/API-supported-green.png)|
|head| ![](images/API-supported-green.png)|
|reduce| ![](images/API-supported-green.png)|
|show||
|take| ![](images/API-supported-green.png)|
|takeAsList|![](images/API-supported-green.png)|
|toLocalIterator||

### Dataset API - Basic Dataset functions

|Operation|Supported?|
|---------|---------|
|as||
|cache| ![](images/API-supported-green.png)|
|checkpoint| ![](images/API-supported-green.png)|
|columns||
|createGlobalTempView||
|createOrReplaceTempView||
|createTempView||
|dtypes||
|explain||
|inputFiles||
|isLocal||
|javaRDD||
|persist| ![](images/API-supported-green.png)|
|printSchema||
|rdd||
|schema||
|storageLevel||
|toDF||
|toJavaRDD||
|unpersist|![](images/API-supported-green.png)|
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
|distinct|![](images/API-supported-green.png)|
|dropDuplicates||
|except||
|filter| ![](images/API-supported-green.png)|
|flatMap| ![](images/API-supported-green.png)|
|groupByKey||
|intersect|![](images/API-supported-green.png)|
|joinWith||
|limit||
|map| ![](images/API-supported-green.png)|
|mapPartitions| ![](images/API-supported-green.png)|
|orderBy||
|randomSplit||
|randomSplitAsList||
|repartition||
|sample||
|select||
|sort||
|sortWithinPartitions||
|transform||
|union|![](images/API-supported-green.png)|
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

### KeyValueGroupedDataset API

|Operation|Supported?|
|---------|---------|
|agg||
|cogroup||
|count||
|flatMapGroups||
|keyAs||
|keys||
|mapGroups||
|mapValues||
|reduceGroups||

# Bugs

Please report any bugs or submit feature requests to [spark-local Github issue tracker](https://github.com/piotr-kalanski/spark-local/issues).

# Continuous Integration

[Build History](https://travis-ci.org/piotr-kalanski/spark-local/builds)

# Contact

piotr.kalanski@gmail.com
