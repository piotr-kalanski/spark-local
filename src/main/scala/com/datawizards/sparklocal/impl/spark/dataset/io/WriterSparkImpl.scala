package com.datawizards.sparklocal.impl.spark.dataset.io

import com.databricks.spark.avro._
import com.datawizards.class2csv
import com.datawizards.dmg.dialects
import com.datawizards.dmg.dialects.Dialect
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.dataset.io.{ModelDialects, Writer, WriterExecutor}
import com.datawizards.sparklocal.datastore._
import com.sksamuel.avro4s.{FromRecord, SchemaFor, ToRecord}
import org.apache.spark.sql._
import org.elasticsearch.spark.sql._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

class WriterSparkImpl[T] extends Writer[T] {

  override def write(ds: DataSetAPI[T]): WriterExecutor[T] = new WriterExecutor[T](ds) {

    override def apply(dataStore: CSVDataStore, saveMode: SaveMode)
                      (implicit ct: ClassTag[T], tt: TypeTag[T], csvEncoder: class2csv.CsvEncoder[T], encoder: Encoder[T]): Unit = {

      var df = mapDataFrameColumns(ds.toDataset.toDF, ModelDialects.CSV)
      if(dataStore.columns.nonEmpty) {
        df = df.toDF(dataStore.columns: _*)
      }

      addPartitioning(
        df
          .write
          .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
          .option("header", dataStore.header.toString)
          .option("delimiter", dataStore.delimiter.toString)
          .option("quote", dataStore.quote.toString)
          .option("escape", dataStore.escape.toString)
          //.option("charset", dataStore.charset)
          .mode(saveMode)
        )
        .csv(dataStore.path)
    }

    override def apply(dataStore: JsonDataStore, saveMode: SaveMode)
                      (implicit encoder: Encoder[T], tt: TypeTag[T]): Unit =
        addPartitioning(
          mapDataSetToDataFrameWithTargetColumns(ds, ModelDialects.JSON)
            .write
            .mode(saveMode)
        )
        .json(dataStore.path)

    override def apply(dataStore: ParquetDataStore, saveMode: SaveMode)
                      (implicit tt: TypeTag[T], s: SchemaFor[T], fromR: FromRecord[T], toR: ToRecord[T], encoder: Encoder[T]): Unit =
        addPartitioning(
          mapDataSetToDataFrameWithTargetColumns(ds, ModelDialects.Parquet)
            .write
            .mode(saveMode)
        )
        .parquet(dataStore.path)

    override def apply(dataStore: AvroDataStore, saveMode: SaveMode)
                      (implicit tt: TypeTag[T], s: SchemaFor[T], r: ToRecord[T], encoder: Encoder[T]): Unit =
        addPartitioning(
          mapDataSetToDataFrameWithTargetColumns(ds, ModelDialects.Avro)
            .write
            .mode(saveMode)
        )
        .avro(dataStore.path)

    override def apply(dataStore: HiveDataStore, saveMode: SaveMode)
                      (implicit tt: TypeTag[T], s: SchemaFor[T], r: ToRecord[T], encoder: Encoder[T]): Unit =
        addPartitioning(
          mapDataSetToDataFrameWithTargetColumns(ds, dialects.Hive)
            .write
            .mode(saveMode)
        )
        .saveAsTable(dataStore.fullTableName)

    override protected def writeToJdbc(dataStore: JdbcDataStore)
                      (implicit ct: ClassTag[T], tt: TypeTag[T], jdbcEncoder: com.datawizards.class2jdbc.JdbcEncoder[T], encoder: Encoder[T]): Unit = {
      Class.forName(dataStore.driverClassName)
      mapDataSetToDataFrameWithTargetColumns(ds, dataStore.dialect)
        .write
        .mode("append")
        .jdbc(dataStore.url, dataStore.fullTableName, dataStore.connectionProperties)
    }

    override protected def writeToElasticsearch(dataStore: ElasticsearchDataStore)
                                               (implicit ct: ClassTag[T], tt: TypeTag[T], encoder: Encoder[T]): Unit =
      mapDataSetToDataFrameWithTargetColumns(ds, dialects.Elasticsearch)
        .saveToEs(dataStore.elasticsearchResourceName, dataStore.getConfigForSparkWriter)

    private def mapDataSetToDataFrameWithTargetColumns(ds: DataSetAPI[T], dialect: Dialect)
                                                      (implicit tt: TypeTag[T], encoder: Encoder[T]): DataFrame =
      mapDataFrameColumns(ds.toDataset.toDF(), dialect)

    private def mapDataFrameColumns(df: DataFrame, dialect: Dialect)
                                                      (implicit tt: TypeTag[T], encoder: Encoder[T]): DataFrame =
      df.toDF(extractTargetColumns(dialect):_*)

    private def addPartitioning(writer: DataFrameWriter[Row]): DataFrameWriter[Row] = {
      if(partitioningColumns.isEmpty) writer
      else writer.partitionBy(partitioningColumns.get:_*)
    }

  }

}
