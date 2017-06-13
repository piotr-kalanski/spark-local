package com.datawizards.sparklocal

import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.impl.scala.parallellazy.ParallelLazySeq
import com.datawizards.sparklocal.rdd.RDDAPI
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest.FunSuite

import scala.collection.GenIterable
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.math.Ordering
import scala.math.Ordering.Boolean

trait SparkLocalBaseTest extends FunSuite {
  lazy val spark: SparkSession = {
    val r = SparkSession.builder().master("local").getOrCreate()
    r.sparkContext.setCheckpointDir("checkpoints/")
    r
  }
  lazy val sc: SparkContext = spark.sparkContext
  lazy val sqlContext: SQLContext = spark.sqlContext

  implicit def genIterableOrdering[T](implicit ord: Ordering[T]): Ordering[GenIterable[T]] =
    new Ordering[GenIterable[T]] {
      def compare(x: GenIterable[T], y: GenIterable[T]): Int = {
        val xe = x.iterator
        val ye = y.iterator

        while (xe.hasNext && ye.hasNext) {
          val res = ord.compare(xe.next(), ye.next())
          if (res != 0) return res
        }

        Boolean.compare(xe.hasNext, ye.hasNext)
      }
    }

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
    assertResult(expected.sorted(ord)) {
      ds.collect().sorted(ord)
    }
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

    val scalaEagerImpl = op(DataSetAPI(data))
    val scalaLazyImpl = op(DataSetAPI(data.view))
    val scalaParallelImpl = op(DataSetAPI(data.par))
    val scalaParallelLazyImpl = op(DataSetAPI(new ParallelLazySeq(data.par)))
    val sparkImpl = op(DataSetAPI(ds))

    assert(eq(scalaEagerImpl, sparkImpl))
    assert(eq(scalaEagerImpl, scalaLazyImpl))
    assert(eq(scalaEagerImpl, scalaParallelImpl))
    assert(eq(scalaEagerImpl, scalaParallelLazyImpl))
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
      case (d1,d2) =>
        val d1c = d1.collect().sorted(ord)
        val d2c = d2.collect().sorted(ord)
        (d1c sameElements d2c) || {println(d1c.mkString(",")); println(d2c.mkString(",")); false}
    }
  }

  /**
    * Assert two Datasets are equal.
    *
    * Checks that Datasets contains the same elements after sorting
    *
    * @param ord ordering for sorting result
    */
  def assertDatasetEquals[T](ds1: DataSetAPI[T], ds2: DataSetAPI[T])(implicit ord: Ordering[T]): Unit = {
    val d1c = ds1.collect().sorted(ord)
    val d2c = ds2.collect().sorted(ord)
    assert((d1c sameElements d2c) || {println(d1c.mkString(",")); println(d2c.mkString(",")); false})
  }

  /**
    * Verifies that RDD has the same elements as expected result
    *
    * @param rdd result RDD
    * @param expected expected result
    */
  def assertRDDOperationResult[T, Result](rdd: RDDAPI[T])(expected: Array[Result]): Unit = {
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

    val scalaEagerImpl = op(RDDAPI(data))
    val scalaLazyImpl = op(RDDAPI(data.view))
    val scalaParallelImpl = op(RDDAPI(data.par))
    val sparkImpl = op(RDDAPI(rdd))
    val scalaParallelLazyImpl = op(RDDAPI(new ParallelLazySeq(data.par)))

    assert(eq(scalaEagerImpl, sparkImpl))
    assert(eq(scalaEagerImpl, scalaLazyImpl))
    assert(eq(scalaEagerImpl, scalaParallelImpl))
    assert(eq(scalaEagerImpl, scalaParallelLazyImpl))
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

  /**
    * Assert two RDD are equal.
    *
    * Checks that RDD contains the same elements after sorting
    *
    * @param ord ordering for sorting result
    */
  def assertRDDEquals[T](rdd1: RDDAPI[T], rdd2: RDDAPI[T])(implicit ord: Ordering[T]): Unit = {
    val d1c = rdd1.collect().sorted(ord)
    val d2c = rdd2.collect().sorted(ord)
    assert((d1c sameElements d2c) || {println(d1c.mkString(",")); println(d2c.mkString(",")); false})
  }

}
