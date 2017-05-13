package com.datawizards.sparklocal.dataset.io

import com.datawizards.class2csv.CsvEncoder
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.datastore._
import org.apache.spark.sql.SaveMode

import scala.reflect.ClassTag

abstract class WriterExecutor[T](ds: DataSetAPI[T]) {
  def apply(dataStore: StdoutStore, saveMode: SaveMode): Unit
  def apply(dataStore: CSVDataStore, saveMode: SaveMode)
           (implicit ct: ClassTag[T], enc: CsvEncoder[T]): Unit
  def apply(dataStore: JsonDataStore, saveMode: SaveMode): Unit
  def apply(dataStore: ParquetDataStore, saveMode: SaveMode): Unit
  def apply(dataStore: AvroDataStore, saveMode: SaveMode): Unit
}
