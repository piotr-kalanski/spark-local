package com.datawizards.sparklocal.dataset.io

import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.datastore._

trait Writer {
  def writeGeneric[T](ds: DataSetAPI[T], dataStore: DataStore[T]): Unit = dataStore match {
    case d:StdoutStore[T] => write(ds, d)
    case d:CSVDataStore[T] => write(ds, d)
    case d:JsonDataStore[T] => write(ds, d)
    case d:ParquetDataStore[T] => write(ds, d)
    case d:AvroDataStore[T] => write(ds, d)
  }
  def write[T](ds: DataSetAPI[T], dataStore: StdoutStore[T]): Unit
  def write[T](ds: DataSetAPI[T], dataStore: CSVDataStore[T]): Unit
  def write[T](ds: DataSetAPI[T], dataStore: JsonDataStore[T]): Unit
  def write[T](ds: DataSetAPI[T], dataStore: ParquetDataStore[T]): Unit
  def write[T](ds: DataSetAPI[T], dataStore: AvroDataStore[T]): Unit
}
