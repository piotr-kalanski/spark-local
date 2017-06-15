package com.datawizards.sparklocal.impl.scala.accumulator

import java.util.Collections

import com.datawizards.sparklocal.accumulator.{AccumulatorV2API, CollectionAccumulatorAPI}

class CollectionAccumulatorAPIScalaImpl[T](name: Option[String]=None)
  extends AccumulatorV2APIScalaImpl[T, java.util.List[T]](name)
    with CollectionAccumulatorAPI[T] {

  private val _list: java.util.List[T] = Collections.synchronizedList(new java.util.ArrayList[T]())

  override def isZero: Boolean = _list.isEmpty

  override def copy(): AccumulatorV2API[T, java.util.List[T]] = {
    val newAcc = new CollectionAccumulatorAPIScalaImpl[T]
    _list.synchronized {
      newAcc._list.addAll(_list)
    }
    newAcc
  }

  override def reset(): Unit = _list.clear()

  override def add(v: T): Unit = _list.add(v)

  override def merge(other: AccumulatorV2API[T, java.util.List[T]]): Unit = other match {
    case o:CollectionAccumulatorAPIScalaImpl[T] => _list.addAll(o.value)
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: java.util.List[T] = _list.synchronized {
    java.util.Collections.unmodifiableList(new java.util.ArrayList[T](_list))
  }
}