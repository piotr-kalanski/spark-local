# spark-local

API enabling switching between Spark execution engine and local fast implementation based on Scala collections.

# Goals

- Speed up unit testing when using Spark
- Enable possibility to switch between Spark execution engine and Scala collections depending on use case, especially size of data

# Example

```scala
import com.datawizards.sparklocal.DataSetAPI
import org.apache.spark.sql.SparkSession

object Example1 {

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

# Bugs

Please report any bugs or submit feature requests to [spark-local Github issue tracker](https://github.com/piotr-kalanski/spark-local/issues).

# Continuous Integration

[![Build Status](https://api.travis-ci.org/piotr-kalanski/spark-local.png?branch=development)](https://api.travis-ci.org/piotr-kalanski/spark-local.png?branch=development)

[Build History](https://travis-ci.org/piotr-kalanski/spark-local/builds)

# Code coverage

[![codecov.io](http://codecov.io/github/piotr-kalanski/spark-local/coverage.svg?branch=development)](http://codecov.io/github/piotr-kalanski/spark-local/coverage.svg?branch=development)

# Contact

piotr.kalanski@gmail.com
