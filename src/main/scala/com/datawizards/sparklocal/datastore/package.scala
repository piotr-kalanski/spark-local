package com.datawizards.sparklocal

import java.util.Properties

import com.datawizards.dmg.dialects.{Dialect, H2Dialect, MySQLDialect}
import com.datawizards.sparklocal.datastore.es._

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
    def localFilePath: String = localDirectoryPath

    private def localHiveWarehouseDirectoryPath = "spark-warehouse/"
  }
  trait JdbcDataStore extends DBDataStore {
    def url: String
    def connectionProperties: Properties
    def driverClassName: String
    def dialect: Dialect
  }
  case class H2DataStore(url: String, database: String, table: String, connectionProperties: Properties) extends JdbcDataStore {
    override def driverClassName: String = "org.h2.Driver"
    override def dialect = H2Dialect
  }
  case class MySQLDataStore(url: String, database: String, table: String, connectionProperties: Properties) extends JdbcDataStore {
    override def driverClassName: String = "com.mysql.jdbc.Driver"
    override def dialect = MySQLDialect
  }

  trait ElasticsearchDataStore extends DataStore {
    def host: String
    def elasticsearchIndexName: String
    def elasticsearchTypeName: String

    def elasticsearchResourceName: String = {
      elasticsearchIndexName + "/" + elasticsearchTypeName
    }

    private [sparklocal] def getConfigForSparkWriter: Map[String, String] =
      Map("es.nodes" -> host)

    private [sparklocal] def getRestAPIURL: String =
      s"http://$host:9200"
  }

  case class ElasticsearchSimpleIndexDataStore(host: String, elasticsearchIndexName: String, elasticsearchTypeName: String)
    extends ElasticsearchDataStore

  case class ElasticsearchTimeSeriesIndexDataStore(
                                                    host: String,
                                                    elasticsearchIndexPrefix: String,
                                                    elasticsearchTypeName: String,
                                                    elasticsearchTimeSeriesGranularity: ElasticsearchTimeSeriesGranularity,
                                                    elasticsearchTimeSeriesIndexDate: ElasticsearchTimeSeriesIndexDate
                                                  )
    extends ElasticsearchDataStore
  {

    def elasticsearchIndexName: String = {
      elasticsearchIndexPrefix +
        "-" + elasticsearchTimeSeriesGranularity.getDateGranularity +
        "-" + elasticsearchTimeSeriesGranularity.getDateFormat.format(elasticsearchTimeSeriesIndexDate.getDate)
    }

  }
}
