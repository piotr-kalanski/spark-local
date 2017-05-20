package com.datawizards.sparklocal

import java.util.Properties

package object datastore {

  sealed trait DataStore

  case class Stdout(rows:Int = 20)

  trait FileDataStore extends DataStore {
    val path: String
    val extension: String
  }
  case class CSVDataStore(
    path: String,
    delimiter: Char = ',',
    header: Boolean = true,
    columns: Seq[String] = Seq.empty,
    escape: Char = '"',
    quote: Char = '"'
  ) extends FileDataStore {
    override val extension: String = ".csv"
  }
  case class JsonDataStore(path: String) extends FileDataStore {
    override val extension: String = ".json"
  }
  case class ParquetDataStore(path: String) extends FileDataStore {
    override val extension: String = ".parquet"
  }
  case class AvroDataStore(path: String) extends FileDataStore{
    override val extension: String = ".avro"
  }

  trait DBDataStore extends DataStore {
    val database: String
    val table: String

    def fullTableName: String = database + "." + table
  }
  case class HiveDataStore(database: String, table: String) extends DBDataStore {
    def localDirectoryPath: String = localHiveWarehouseDirectoryPath + database + "/" + table
    def localFilePath: String = localDirectoryPath + "/data.avro"

    private def localHiveWarehouseDirectoryPath = "spark-warehouse/"
  }
  case class JdbcDataStore(url: String, database: String, table: String, connectionProperties: Properties, driverClassName: String) extends DBDataStore

}
