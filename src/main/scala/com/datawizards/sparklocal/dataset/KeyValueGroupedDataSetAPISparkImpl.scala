package com.datawizards.sparklocal.dataset

import org.apache.spark.sql.KeyValueGroupedDataset

import scala.reflect.runtime.universe.TypeTag

class KeyValueGroupedDataSetAPISparkImpl[K: TypeTag, T](data: KeyValueGroupedDataset[K, T]) extends KeyValueGroupedDataSetAPI[K, T] {
  override def count(): DataSetAPI[(K, Long)] = DataSetAPI(data.count())
}
