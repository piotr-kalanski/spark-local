package com.datawizards.sparklocal

package object datastore {

  sealed trait DataStore
  trait FileDataStore extends DataStore {
    val path: String
  }

  case class StdoutStore()

  case class CSVDataStore(
    path: String,
    delimiter: Char = ',',
    header: Boolean = true,
    columns: Seq[String] = Seq.empty,
    escape: Char = '"',
    quote: Char = '"'
  ) extends FileDataStore
  case class JsonDataStore(path: String) extends FileDataStore
  case class ParquetDataStore(path: String) extends FileDataStore
  case class AvroDataStore(path: String) extends FileDataStore

}
