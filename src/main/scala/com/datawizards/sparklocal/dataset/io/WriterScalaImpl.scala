package com.datawizards.sparklocal.dataset.io

import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.datastore._

object WriterScalaImpl extends Writer {
  override def write[T](ds: DataSetAPI[T], dataStore: StdoutStore[T]): Unit = ???

  override def write[T](ds: DataSetAPI[T], dataStore: CSVDataStore[T]): Unit = ???

  override def write[T](ds: DataSetAPI[T], dataStore: JsonDataStore[T]): Unit = ???

  override def write[T](ds: DataSetAPI[T], dataStore: ParquetDataStore[T]): Unit = ???

  override def write[T](ds: DataSetAPI[T], dataStore: AvroDataStore[T]): Unit = ???
}
