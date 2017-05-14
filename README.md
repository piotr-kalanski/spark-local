# spark-local

API enabling switching between Spark execution engine and local implementation based on Scala collections.

[![Build Status](https://api.travis-ci.org/piotr-kalanski/spark-local.png?branch=development)](https://api.travis-ci.org/piotr-kalanski/spark-local.png?branch=development)
[![codecov.io](http://codecov.io/github/piotr-kalanski/spark-local/coverage.svg?branch=development)](http://codecov.io/github/piotr-kalanski/spark-local/coverage.svg?branch=development)
[<img src="https://img.shields.io/maven-central/v/com.github.piotr-kalanski/spark-local_2.11.svg?label=latest%20release"/>](http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22spark-local_2.11%22)
[![Stories in Ready](https://badge.waffle.io/piotr-kalanski/spark-local.png?label=Ready)](https://waffle.io/piotr-kalanski/spark-local)
[![License](http://img.shields.io/:license-Apache%202-red.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)

# Table of contents

- [Goals](#goals)
- [Getting started](#getting-started)
- [Examples](#examples)
- [Supported operations](#supported-operations)
- [IO operations](#io-operations)
- [Supported Spark versions](#supported-spark-versions)

# Goals

- Speed up unit testing when using Spark
- Enable possibility to switch between Spark execution engine and Scala collections depending on use case, especially size of data

# Getting started

Include dependency:

```scala
"com.github.piotr-kalanski" % "spark-local_2.11" % "0.3.0"
```

or

```xml
<dependency>
    <groupId>com.github.piotr-kalanski</groupId>
    <artifactId>spark-local_2.11</artifactId>
    <version>0.3.0</version>
</dependency>
```

# Examples

##

## RDD API

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

## Dataset API

### Simple example

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

### Example report

```scala
case class Person(id: Int, name: String, gender: String)
case class WorkExperience(personId: Int, year: Int, title: String)
case class HRReport(year: Int, title: String, gender: String, count: Int)

object ExampleHRReport {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._

    val people = SampleData.people
    val peopleDs = people.toDS()
    val experience = SampleData.experience
    val experienceDs = experience.toDS()


    calculateReport(DataSetAPI(people), DataSetAPI(experience))
    calculateReport(DataSetAPI(peopleDs), DataSetAPI(experienceDs))
  }

  def calculateReport(people: DataSetAPI[Person], workExperience: DataSetAPI[WorkExperience]): DataSetAPI[HRReport] = {
    workExperience
      .join(people)(_.personId, _.id)
      .groupByKey(wp => (wp._1.year, wp._1.title, wp._2.gender))
      .mapGroups{case ((year, title, gender), vals) => HRReport(year, title, gender, vals.size)}
  }

}
```

# Supported operations

## RDD API

### RDD API - basic actions and transformations

|Operation|Supported?|
|---------|---------|
|aggregate||
|cache|![](images/API-supported-green.png)|
|cartesian|![](images/API-supported-green.png)|
|checkpoint|![](images/API-supported-green.png)|
|coalesce|![](images/API-supported-green.png)|
|collect| ![](images/API-supported-green.png)|
|count| ![](images/API-supported-green.png)|
|countApprox||
|countApproxDistinct||
|countByValue||
|countByValueApprox||
|dependencies||
|distinct|![](images/API-supported-green.png)|
|filter| ![](images/API-supported-green.png)|
|first|![](images/API-supported-green.png)|
|flatMap| ![](images/API-supported-green.png)|
|fold| ![](images/API-supported-green.png)|
|foreach| ![](images/API-supported-green.png)|
|foreachPartition| ![](images/API-supported-green.png)|
|getCheckpointFile||
|getNumPartitions||
|getStorageLevel||
|glom||
|groupBy|![](images/API-supported-green.png)|
|id||
|intersection| ![](images/API-supported-green.png)|
|isCheckpointed||
|isEmpty| ![](images/API-supported-green.png)|
|iterator||
|keyBy|![](images/API-supported-green.png)|
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
|randomSplit|![](images/API-supported-green.png)|
|reduce| ![](images/API-supported-green.png)|
|repartition|![](images/API-supported-green.png)|
|sample|![](images/API-supported-green.png)|
|saveAsObjectFile||
|saveAsTextFile||
|setName||
|sortBy| ![](images/API-supported-green.png)|
|subtract|![](images/API-supported-green.png)|
|take| ![](images/API-supported-green.png)|
|takeOrdered|![](images/API-supported-green.png)|
|takeSample|![](images/API-supported-green.png)|
|toDebugString||
|toJavaRDD||
|toLocalIterator||
|top|![](images/API-supported-green.png)|
|treeAggregate||
|treeReduce||
|union| ![](images/API-supported-green.png)|
|unpersist|![](images/API-supported-green.png)|
|zip| ![](images/API-supported-green.png)|
|zipPartitions||
|zipWithIndex| ![](images/API-supported-green.png)|
|zipWithUniqueId||

### Pair RDD API

|Operation|Supported?|
|---------|---------|
|aggregateByKey|![](images/API-supported-green.png)|
|cogroup|![](images/API-supported-green.png)|
|collectAsMap|![](images/API-supported-green.png)|
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
|partitionBy|![](images/API-supported-green.png)|
|reduceByKey|![](images/API-supported-green.png)|
|reduceByKeyLocally|![](images/API-supported-green.png)|
|rightOuterJoin|![](images/API-supported-green.png)|
|sampleByKey||
|sampleByKeyExact||
|saveAsHadoopDataset||
|saveAsHadoopFile||
|saveAsNewAPIHadoopDataset||
|subtractByKey|![](images/API-supported-green.png)|
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
|rdd|![](images/API-supported-green.png)|
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
|groupByKey|![](images/API-supported-green.png)|
|intersect|![](images/API-supported-green.png)|
|joinWith||
|limit|![](images/API-supported-green.png)|
|map|![](images/API-supported-green.png)|
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
|cogroup|![](images/API-supported-green.png)|
|count|![](images/API-supported-green.png)|
|flatMapGroups|![](images/API-supported-green.png)|
|keyAs||
|keys|![](images/API-supported-green.png)|
|mapGroups|![](images/API-supported-green.png)|
|mapValues|![](images/API-supported-green.png)|
|reduceGroups|![](images/API-supported-green.png)|

### Dataset - additional API
|Operation|Supported?|
|---------|---------|
|join|![](images/API-supported-green.png)|
|leftOuterJoin|![](images/API-supported-green.png)|
|rightOuterJoin|![](images/API-supported-green.png)|
|fullOuterJoin|![](images/API-supported-green.png)|

# IO operations

Library provides dedicated API for input/output operations with implementation for Spark and Scala collections.

## CSV

### Read CSV file

```scala
val reader: Reader = ReaderScalaImpl // Scala implementation
//val reader: Reader = ReaderSparkImpl // Spark implementation

reader.read[Person](
    CSVDataStore(
        path = "people.csv",
        delimiter = ';',
        header = false,
        columns = Seq("name","age")
    )
)
```

### Write to CSV file

```scala
ds.write(CSVDataStore(file), SaveMode.Overwrite)
```

## JSON

### Read JSON file

```scala
reader.read[Person](JsonDataStore("people.json"))
```

### Write to JSON file

```scala
ds.write(JsonDataStore("people.json"), SaveMode.Overwrite)
```

## Avro

Current implementation produces different binary files for Spark and Scala.
Spark by default compress files with snappy and spark-local implementation is based on: https://github.com/sksamuel/avro4s, which saves data without compression.

### Read Avro file

```scala
reader.read[Person](AvroDataStore("people.avro"))
```

### Write to Avro file

```scala
ds.write(AvroDataStore("people.avro"), SaveMode.Overwrite)
```

# Supported Spark versions

|spark-local|Spark version|
|-----------|-------------|
|0.4        |2.1.0        |
|0.3        |2.1.0        |
|0.2        |2.1.0        |
|0.1        |2.1.0        |

# Bugs

Please report any bugs or submit feature requests to [spark-local Github issue tracker](https://github.com/piotr-kalanski/spark-local/issues).

# Continuous Integration

[Build History](https://travis-ci.org/piotr-kalanski/spark-local/builds)

# Contact

piotr.kalanski@gmail.com
