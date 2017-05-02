package com.datawizards.sparklocal.dataset

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

trait KeyValueGroupedDataSetAPI[K, V] {
  def count(): DataSetAPI[(K, Long)]
  def mapGroups[U: ClassTag: TypeTag](f: (K, Iterator[V]) => U): DataSetAPI[U]
  def reduceGroups(f: (V, V) => V): DataSetAPI[(K, V)]
  def mapValues[W: ClassTag: TypeTag](func: V => W): KeyValueGroupedDataSetAPI[K, W]
}
