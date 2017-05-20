package com.datawizards.sparklocal

import java.util.Properties

package object datastore {

  sealed trait DataStore

  case class Stdout(rows:Int = 20)

  trait FileDataStore extends DataStore {
    val path: String
  }
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

  trait DBDataStore extends DataStore {
    val database: String
    val table: String

    def fullTableName: String = database + "." + table
  }
  case class HiveDataStore(database: String, table: String) extends DBDataStore {
    def localDirectoryPath: String = hiveWarehouseDirectoryPath + database + "/" + table
    def localFilePath: String = localDirectoryPath + "/data.avro"

    private def hiveWarehouseDirectoryPath = "spark-warehouse/"
  }
  case class JdbcDataStore(url: String, database: String, table: String, connectionProperties: Properties, driverClassName: String) extends DBDataStore

}
