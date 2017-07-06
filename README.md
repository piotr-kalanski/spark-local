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
- [IO operations](#io-operations)
  - [Supported formats](#supported-formats)
  - [Data model versioning](#data-model-versioning)
  - [Column mapping](#column-mapping)
- [Aggregations](#aggregations)
- [Supported Spark versions](#supported-spark-versions)
- [Supported Spark operations](doc/SupportedOperations.md)
- [Benchmarks](benchmarks/Benchmarks.md)
- [Contributing](CONTRIBUTING.md)

# Goals

- Speed up unit testing for Spark applications
- Enable switching between Spark execution engine and Scala collections depending on use case, especially size of data without changing implementation

# Getting started

## Spark 2.1.1

Include dependency:

```scala
"com.github.piotr-kalanski" % "spark-local_2.1.1_2.11" % "0.6.0"
```

or

```xml
<dependency>
    <groupId>com.github.piotr-kalanski</groupId>
    <artifactId>spark-local_2.1.1_2.11</artifactId>
    <version>0.6.0</version>
</dependency>
```

## Spark 2.1.0

Include dependency:

```scala
"com.github.piotr-kalanski" % "spark-local_2.1.0_2.11" % "0.6.0"
```

or

```xml
<dependency>
    <groupId>com.github.piotr-kalanski</groupId>
    <artifactId>spark-local_2.1.0_2.11</artifactId>
    <version>0.6.0</version>
</dependency>
```

# Examples

## Creating Session

Entry point for library is session object which is similar to SparkSession object from Apache Spark.

Process of creating Session is also similar to Apache Spark.

When creating Session object you can choose between different execution engines. Currently supported:

- Spark - wrapper on Spark, which can be used at production data volumes
- ScalaEager - implementation based on Scala collection with eager transformations, which makes it fast for unit testing
- ScalaLazy - implementation based on Scala collection with lazy transformations, dedicated for working with small/mid size data
- ScalaParallel - implementation based on Scala parallel collection with eager transformations
- ScalaParallelLazy - implementation based on Scala parallel collection with lazy transformations

```scala
import com.datawizards.sparklocal.session.ExecutionEngine.ExecutionEngine
import com.datawizards.sparklocal.session.{ExecutionEngine, SparkSessionAPI}

// just change this value to start using different execution engine
val engine = ExecutionEngine.ScalaEager

val session = SparkSessionAPI
      .builder(engine)
      .master("local")
      .getOrCreate()

val ds = session.read[Person](CSVDataStore(file))
```

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

# IO operations

Library provides dedicated API for input/output operations with implementation for Spark and Scala collections.

## Supported formats

Supported formats:
- CSV
- JSON
- Parquet
- Avro
- Hive
- JDBC
  - H2
  - MySQL
- Elasticsearch

### CSV

#### Read CSV file

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

#### Write to CSV file

```scala
ds.write(CSVDataStore(file), SaveMode.Overwrite)
```

### JSON

#### Read JSON file

```scala
reader.read[Person](JsonDataStore("people.json"))
```

#### Write to JSON file

```scala
ds.write(JsonDataStore("people.json"), SaveMode.Overwrite)
```

### Avro

Current implementation produces different binary files for Spark and Scala.
Spark by default compress files with snappy and spark-local implementation is based on: https://github.com/sksamuel/avro4s, which saves data without compression.

#### Read Avro file

```scala
reader.read[Person](AvroDataStore("people.avro"))
```

#### Write to Avro file

```scala
ds.write(AvroDataStore("people.avro"), SaveMode.Overwrite)
```

### Parquet

#### Read Parquet file

```scala
reader.read[Person](ParquetDataStore("people.parquet"))
```

#### Write to Parquet file

```scala
ds.write(ParquetDataStore("people.parquet"), SaveMode.Overwrite)
```

### Hive

#### Read Hive table

```scala
reader.read[Person](HiveDataStore("db", "table"))
```

#### Write to Hive table

```scala
ds.write(HiveDataStore("db", "table"), SaveMode.Overwrite)
```

### JDBC

#### Read JDBC table

```scala

val database = "public"
val table = "people"
val properties = new java.util.Properties()
reader.read[Person](H2DataStore(connectionString, database, table, properties))
```

#### Write to JDBC table

```scala
ds.write(H2DataStore(connectionString, database, table, properties), SaveMode.Append)
```

### Elasticsearch

#### Write to Elasticsearch index

```scala
val indexName = "people"
val typeName = "person"
ds.write(ElasticsearchSimpleIndexDataStore("localhost", indexName, typeName), SaveMode.Append)
```

## Data model versioning

Library supports reading data using old and new version of data model.

Reading with old version of data model is straightforward, just existing fields will be read.
On other hand, when reading with new version of data model, new columns should be `Option` type and value `None` is assigned to those fields.

**Example**

```scala
import com.datawizards.sparklocal.datastore.CSVDataStore
import com.datawizards.sparklocal.session.{ExecutionEngine, SparkSessionAPI}
import org.apache.spark.sql.SaveMode

case class Person(name: String, age: Int)
case class PersonV2(name: String, age: Int, title: Option[String])
case class PersonV3(name: String, age: Int, title: Option[String], salary: Option[Long])

val session = SparkSessionAPI
      .builder(ExecutionEngine.ScalaEager)
      .master("local")
      .getOrCreate()

import session.implicits._

val peopleV2 = session.createDataset(Seq(
    PersonV2("p1", 10, Some("Mr")),
    PersonV2("p2", 20, Some("Ms")),
    PersonV2("p3", 30, None),
    PersonV2("p4", 40, Some("Mr"))
))

val dataStore = CSVDataStore("people_v2.csv")
peopleV2.write(dataStore, SaveMode.Overwrite)

// Read old version:
val peopleV1 = session.read[Person](dataStore)

// Read new version:
val peopleV3 = session.read[PersonV3](dataStore)

peopleV1.show()
peopleV3.show()
```

```
+----+---+
|name|age|
+----+---+
|p1  |10 |
|p2  |20 |
|p3  |30 |
|p4  |40 |
+----+---+

+----+---+-----+------+
|name|age|title|salary|
+----+---+-----+------+
|p1  |10 |Mr   |      |
|p2  |20 |Ms   |      |
|p3  |30 |     |      |
|p4  |40 |Mr   |      |
+----+---+-----+------+
```

## Column mapping

Library supports changing name of fields when writing and reading data.

Mapping is provided using `column` annotation from project: https://github.com/piotr-kalanski/data-model-generator.

**Example**

```scala
import com.datawizards.dmg.annotations.column
import com.datawizards.sparklocal.dataset.io.ModelDialects
import com.datawizards.sparklocal.datastore.JsonDataStore
import com.datawizards.sparklocal.session.{ExecutionEngine, SparkSessionAPI}
import org.apache.spark.sql.SaveMode

case class PersonWithMapping(
    @column("personName", dialect = ModelDialects.JSON)
    name: String,
    @column("personAge", dialect = ModelDialects.JSON)
    age: Int
)

val session = SparkSessionAPI
      .builder(ExecutionEngine.ScalaEager)
      .master("local")
      .getOrCreate()

import session.implicits._

val people = session.createDataset(Seq(
    PersonWithMapping("p1", 10),
    PersonWithMapping("p2", 20),
    PersonWithMapping("p3", 30),
    PersonWithMapping("p4", 40)
))

val dataStore = JsonDataStore("people_mapping.json")
people.write(dataStore, SaveMode.Overwrite)

```

Name of fields in JSON file are consistent with column names provided in annotations:

```json
{"personName":"p1","personAge":10}
{"personName":"p2","personAge":20}
{"personName":"p3","personAge":30}
{"personName":"p4","personAge":40}
```

Reading data:
```scala
val people2 = session.read[PersonWithMapping](dataStore)
people2.show()
```

Names of columns are the same as case class fields:
```
+----+---+
|name|age|
+----+---+
|p1  |10 |
|p2  |20 |
|p3  |30 |
|p4  |40 |
+----+---+
```

# Aggregations

Library provides custom type-safe API for aggregations.

Example operations:

```scala
import com.datawizards.sparklocal.dataset.agg._

ds
    .groupByKey(_.name)
    .agg(sum(_.age), count(), mean(_.age), max(_.age))
```

# Supported Spark versions

|spark-local|Spark version|
|-----------|-------------|
|0.7        |2.1.1<br/>2.1.0  |
|0.6        |2.1.1<br/>2.1.0  |
|0.5        |2.1.0        |
|0.4        |2.1.0        |
|0.3        |2.1.0        |
|0.2        |2.1.0        |
|0.1        |2.1.0        |

# Bugs

Please report any bugs to [spark-local Github issue tracker](https://github.com/piotr-kalanski/spark-local/issues).

# Continuous Integration

[Build History](https://travis-ci.org/piotr-kalanski/spark-local/builds)

# Contact

piotr.kalanski@gmail.com
