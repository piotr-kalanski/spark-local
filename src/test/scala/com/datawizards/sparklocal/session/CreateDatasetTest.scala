package com.datawizards.sparklocal.session

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.rdd.RDDAPI
import com.datawizards.sparklocal.implicits._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CreateDatasetTest extends SparkLocalBaseTest {

  val data = Array(1,2,3,4,5)

  test("Create DataSet - result") {
    assertDatasetOperationResult(createDataset(ExecutionEngine.ScalaEager)) {
      data
    }
  }

  test("Create DataSet(RDD) - result") {
    assertDatasetOperationResult(createDatasetRDD(ExecutionEngine.ScalaEager)) {
      data
    }
  }

  test("Create DataSet - equals") {
    assertDatasetEquals(
      createDataset(ExecutionEngine.ScalaEager),
      createDataset(ExecutionEngine.ScalaLazy)
    )
    assertDatasetEquals(
      createDataset(ExecutionEngine.ScalaEager),
      createDataset(ExecutionEngine.ScalaParallel)
    )
    assertDatasetEquals(
      createDataset(ExecutionEngine.ScalaEager),
      createDataset(ExecutionEngine.ScalaParallelLazy)
    )
    assertDatasetEquals(
      createDataset(ExecutionEngine.ScalaEager),
      createDataset(ExecutionEngine.Spark)
    )
  }

  test("Create DataSet(RDD) - equals") {
    assertDatasetEquals(
      createDatasetRDD(ExecutionEngine.ScalaEager),
      createDatasetRDD(ExecutionEngine.ScalaLazy)
    )
    assertDatasetEquals(
      createDatasetRDD(ExecutionEngine.ScalaEager),
      createDatasetRDD(ExecutionEngine.ScalaParallel)
    )
    assertDatasetEquals(
      createDatasetRDD(ExecutionEngine.ScalaEager),
      createDatasetRDD(ExecutionEngine.ScalaParallelLazy)
    )
    assertDatasetEquals(
      createDatasetRDD(ExecutionEngine.ScalaEager),
      createDatasetRDD(ExecutionEngine.Spark)
    )
  }

  private def createDataset[Session <: SparkSessionAPI](engine: ExecutionEngine[Session]): DataSetAPI[Int] =
    SparkSessionAPI
      .builder(engine)
      .master("local")
      .getOrCreate()
      .createDataset(data)

  private def createDatasetRDD[Session <: SparkSessionAPI](engine: ExecutionEngine[Session]): DataSetAPI[Int] =
    SparkSessionAPI
      .builder(engine)
      .master("local")
      .getOrCreate()
      .createDataset(RDDAPI(data))

}