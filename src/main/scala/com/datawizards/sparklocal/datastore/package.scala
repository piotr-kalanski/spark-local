package com.datawizards.sparklocal

package object datastore {

  sealed trait DataStore[T]
  trait FileDataStore[T] extends DataStore[T] {
    val path: String
  }

  case class StdoutStore[T]()

  case class CSVDataStore[T](
    path: String,
    delimiter: Char=',',
    header: Boolean = true,
    columns: Seq[String] = Seq.empty,
    escape: Char = '"',
    quote: Char = '"'
  ) extends FileDataStore[T]
  case class JsonDataStore[T](path: String) extends FileDataStore[T]
  case class ParquetDataStore[T](path: String) extends FileDataStore[T]
  case class AvroDataStore[T](path: String) extends FileDataStore[T]

}
