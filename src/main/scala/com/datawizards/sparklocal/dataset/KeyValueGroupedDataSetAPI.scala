package com.datawizards.sparklocal.dataset

trait KeyValueGroupedDataSetAPI[K, T] {
  def count(): DataSetAPI[(K, Long)]
}
