package com.datawizards.sparklocal.impl.scala.parallellazy

import scala.collection.SeqView
import scala.collection.parallel.ParSeq

class ParallelLazySeq[T](data: ParSeq[T]) extends SeqView[T, ParSeq[T]] {
  override def length: Int = data.length
  override protected def underlying: ParSeq[T] = data.repr
  override def iterator: Iterator[T] = data.iterator
  override def apply(idx: Int): T = data.apply(idx)
}
