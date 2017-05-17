package com.datawizards.sparklocal

package object datastore {

  sealed trait DataStore
  trait FileDataStore extends DataStore {
    val path: String
  }
  trait DBDataStore extends DataStore {
    val database: String
    val table: String

    def fullTableName: String = database + "." + table
  }

  case class Stdout(rows:Int = 20)

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

  case class HiveDataStore(database: String, table: String) extends DBDataStore {
    def localDirectoryPath: String = hiveWarehouseDirectoryPath + database + "/" + table
    def localFilePath: String = localDirectoryPath + "/data.avro"

    private def hiveWarehouseDirectoryPath = "spark-warehouse/"
  }
}
