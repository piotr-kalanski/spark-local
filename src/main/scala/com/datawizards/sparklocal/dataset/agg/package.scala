package com.datawizards.sparklocal.dataset

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Encoder, TypedColumn}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.GenIterable

package object agg {
  trait AggregationFunction[T, V] extends Serializable {
    type Buf

    protected def zero: Buf
    protected def reduce(b: Buf, a: T): Buf
    protected def merge(b1: Buf, b2: Buf): Buf
    protected def finish(reduction: Buf): V
    protected def bufferEncoder: Encoder[Buf]
    protected def outputEncoder: Encoder[V]

    protected val aggregator: Aggregator[T, Buf, V] = new Aggregator[T, Buf, V] {
      override def zero: Buf = AggregationFunction.this.zero
      override def reduce(b: Buf, a: T): Buf = AggregationFunction.this.reduce(b, a)
      override def merge(b1: Buf, b2: Buf): Buf = AggregationFunction.this.merge(b1, b2)
      override def finish(reduction: Buf): V = AggregationFunction.this.finish(reduction)
      override def bufferEncoder: Encoder[Buf] = AggregationFunction.this.bufferEncoder
      override def outputEncoder: Encoder[V] = AggregationFunction.this.outputEncoder
    }

    def toTypedColumn: TypedColumn[T, V] = aggregator.toColumn
    def aggregate(iterable: GenIterable[T]): V =
      finish(iterable.aggregate(zero)(reduce, merge))
  }

  class SumFunction[T](value: T => Double) extends AggregationFunction[T, Double] {
    override type Buf = Double
    override protected def zero: Buf = 0.0
    override protected def reduce(b: Buf, a: T): Buf = b + value(a)
    override protected def merge(b1: Buf, b2: Buf): Buf = b1 + b2
    override protected def finish(reduction: Buf): Double = reduction
    override protected def bufferEncoder: Encoder[Double] = ExpressionEncoder[Double]()
    override protected def outputEncoder: Encoder[Double] = ExpressionEncoder[Double]()
  }

  class CountFunction[T] extends AggregationFunction[T, Long] {
    override type Buf = Long
    override protected def zero: Buf = 0L
    override protected def reduce(b: Buf, a: T): Buf = b + 1L
    override protected def merge(b1: Buf, b2: Buf): Buf = b1 + b2
    override protected def finish(reduction: Buf): Long = reduction
    override protected def bufferEncoder: Encoder[Long] = ExpressionEncoder[Long]()
    override protected def outputEncoder: Encoder[Long] = ExpressionEncoder[Long]()
  }

  class AvgFunction[T](value: T => Double) extends AggregationFunction[T, Double] {
    override type Buf = (Double, Long)
    override protected def zero: Buf = (0.0, 0L)
    override protected def reduce(b: Buf, a: T): Buf = (b._1 + value(a), b._2 + 1L)
    override protected def merge(b1: Buf, b2: Buf): Buf = (b1._1+b2._1, b1._2+b2._2)
    override protected def finish(reduction: Buf): Double = reduction._1 / reduction._2
    override protected def bufferEncoder: Encoder[(Double, Long)] = ExpressionEncoder[(Double, Long)]()
    override protected def outputEncoder: Encoder[Double] = ExpressionEncoder[Double]()
  }

  class MaxFunction[T](value: T => Double) extends AggregationFunction[T, Double] {
    override type Buf = Double
    override protected def zero: Buf = Double.MinValue
    override protected def reduce(b: Buf, a: T): Buf = math.max(b, value(a))
    override protected def merge(b1: Buf, b2: Buf): Buf = math.max(b1, b2)
    override protected def finish(reduction: Buf): Double = reduction
    override protected def bufferEncoder: Encoder[Double] = ExpressionEncoder[Double]()
    override protected def outputEncoder: Encoder[Double] = ExpressionEncoder[Double]()
  }

  class MinFunction[T](value: T => Double) extends AggregationFunction[T, Double] {
    override type Buf = Double
    override protected def zero: Buf = Double.MaxValue
    override protected def reduce(b: Buf, a: T): Buf = math.min(b, value(a))
    override protected def merge(b1: Buf, b2: Buf): Buf = math.min(b1, b2)
    override protected def finish(reduction: Buf): Double = reduction
    override protected def bufferEncoder: Encoder[Double] = ExpressionEncoder[Double]()
    override protected def outputEncoder: Encoder[Double] = ExpressionEncoder[Double]()
  }

  def sum[T](value: T => Double): SumFunction[T] = new SumFunction(value)
  def count[T](): CountFunction[T] = new CountFunction
  def avg[T](value: T => Double): AvgFunction[T] = new AvgFunction(value)
  def mean[T](value: T => Double): AvgFunction[T] = avg(value)
  def max[T](value: T => Double): MaxFunction[T] = new MaxFunction(value)
  def min[T](value: T => Double): MinFunction[T] = new MinFunction(value)
}
