package com.datawizards.sparklocal

import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.rdd.RDDAPI
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest.FunSuite

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.math.Ordering

trait SparkLocalBaseTest extends FunSuite {
  lazy val spark: SparkSession = {
    val r = SparkSession.builder().master("local").getOrCreate()
    r.sparkContext.setCheckpointDir("checkpoints/")
    r
  }
  lazy val sc: SparkContext = spark.sparkContext
  lazy val sqlContext: SQLContext = spark.sqlContext

  /**
    * Verifies that Dataset has the same elements as expected result
    *
    * @param ds result Dataset
    * @param expected expected result
    */
  def assertDatasetOperationResult[T](ds: DataSetAPI[T])(expected: Array[T]): Unit = {
    assertResult(expected){
      ds.collect()
    }
  }

  /**
    * Verifies that Dataset has the same elements after sorting as expected result
    *
    * @param ds result Dataset
    * @param expected expected result
    * @param ord ordering that should be used to sort result
    */
  def assertDatasetOperationResultWithSorted[T](ds: DataSetAPI[T])(expected: Array[T])(implicit ord: Ordering[T]): Unit = {
    assert(ds.collect().sorted(ord) sameElements expected.sorted(ord))
  }

  /**
    * Verifies that different implementations (Spark, pure Scala) returns the same result for provided operation and input data
    * <br />
    * Function:
    * <ul>
    * <li>Creates Dataset</li>
    * <li>Run operation on Dataset and Scala implementation</li>
    * <li>Check that result is the same</li>
    * </ul>
    *
    * @param data test data
    * @param op operation that should be performed on Dataset
    */
  def assertDatasetOperationReturnsSameResult[T:ClassTag:TypeTag, Result](data: Seq[T])(op: DataSetAPI[T] => Result): Unit = {
    assertDatasetOperationReturnsSameResultWithEqual(data,op){case(r1,r2) => r1 == r2}
  }

  /**
    * Verifies that different implementations (Spark, pure Scala) returns the same result for provided operation and input data
    * <br />
    * Function:
    * <ul>
    * <li>Creates Dataset</li>
    * <li>Run operation on Dataset and Scala implementation</li>
    * <li>Check that result is the same using provided comparison function (eq)</li>
    * </ul>
    *
    * @param data test data
    * @param op operation that should be performed on Dataset
    * @param eq function to compare result
    */
  def assertDatasetOperationReturnsSameResultWithEqual[T:ClassTag:TypeTag, Result](data: Seq[T], op: DataSetAPI[T] => Result)(eq: ((Result,Result) => Boolean)): Unit = {
    val ds = spark.createDataset(data)(ExpressionEncoder[T]())

    assert(eq(op(DataSetAPI(data)),op(DataSetAPI(ds))))
  }

  /**
    * Verifies that different implementations (Spark, pure Scala) returns the same result (after sorting) for provided operation and input data
    * <br />
    * Function:
    * <ul>
    * <li>Creates Dataset</li>
    * <li>Run operation on Dataset and Scala implementation</li>
    * <li>Sort results using provided ordering</li>
    * <li>Check that sorted result is the same</li>
    * </ul>
    * @param data test data
    * @param op operation that should be performed on Dataset
    * @param ord ordering that should be used to sort result
    */
  def assertDatasetOperationReturnsSameResultWithSorted[T:ClassTag:TypeTag,Result](data: Seq[T])(op: DataSetAPI[T] => DataSetAPI[Result])(implicit ord: Ordering[Result]): Unit = {
    assertDatasetOperationReturnsSameResultWithEqual[T,DataSetAPI[Result]](data, op) {
      case (d1,d2) => d1.collect().sorted(ord) sameElements d2.collect().sorted(ord)
    }
  }

  /**
    * Verifies that RDD has the same elements as expected result
    *
    * @param rdd result RDD
    * @param expected expected result
    */
  def assertRDDOperationResult[T](rdd: RDDAPI[T])(expected: Array[T]): Unit = {
    assertResult(expected){
      rdd.collect()
    }
  }

  /**
    * Verifies that RDD has the same elements after sorting as expected result
    *
    * @param rdd result RDD
    * @param expected expected result
    * @param ord ordering that should be used to sort result
    */
  def assertRDDOperationResultWithSorted[T](rdd: RDDAPI[T])(expected: Array[T])(implicit ord: Ordering[T]): Unit = {
    assert(rdd.collect().sorted(ord) sameElements expected.sorted(ord))
  }

  /**
    * Verifies that different implementations (Spark, pure Scala) returns the same result for provided operation and input data
    * <br />
    * Function:
    * <ul>
    * <li>Creates RDD</li>
    * <li>Run operation on RDD and Scala implementation</li>
    * <li>Check that result is the same</li>
    * </ul>
    *
    * @param data test data
    * @param op operation that should be performed on RDD
    */
  def assertRDDOperationReturnsSameResult[T:ClassTag:TypeTag, Result](data: Seq[T])(op: RDDAPI[T] => Result): Unit = {
    assertRDDOperationReturnsSameResultWithEqual(data,op){case(r1,r2) => r1 == r2}
  }

  /**
    * Verifies that different implementations (Spark, pure Scala) returns the same result for provided operation and input data
    * <br />
    * Function:
    * <ul>
    * <li>Creates RDD</li>
    * <li>Run operation on RDD and Scala implementation</li>
    * <li>Check that result is the same using provided comparison function (eq)</li>
    * </ul>
    *
    * @param data test data
    * @param op operation that should be performed on RDD
    * @param eq function to compare result
    */
  def assertRDDOperationReturnsSameResultWithEqual[T:ClassTag:TypeTag, Result](data: Seq[T], op: RDDAPI[T] => Result)(eq: ((Result,Result) => Boolean)): Unit = {
    val rdd = sc.parallelize(data)

    assert(eq(op(RDDAPI(data)),op(RDDAPI(rdd))))
  }

  /**
    * Verifies that different implementations (Spark, pure Scala) returns the same result (after sorting) for provided operation and input data
    * <br />
    * Function:
    * <ul>
    * <li>Creates RDD</li>
    * <li>Run operation on RDD and Scala implementation</li>
    * <li>Sort results using provided ordering</li>
    * <li>Check that sorted result is the same</li>
    * </ul>
    * @param data test data
    * @param op operation that should be performed on RDD
    * @param ord ordering that should be used to sort result
    */
  def assertRDDOperationReturnsSameResultWithSorted[T:ClassTag:TypeTag,Result](data: Seq[T])(op: RDDAPI[T] => RDDAPI[Result])(implicit ord: Ordering[Result]): Unit = {
    assertRDDOperationReturnsSameResultWithEqual[T,RDDAPI[Result]](data, op) {
      case (d1,d2) => d1.collect().sorted(ord) sameElements d2.collect().sorted(ord)
    }
  }

}
