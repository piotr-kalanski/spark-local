package com.datawizards.sparklocal.impl.spark.dataset.io

import com.databricks.spark.avro._
import com.datawizards.class2csv
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.dataset.io.{Writer, WriterExecutor}
import com.datawizards.sparklocal.datastore._
import com.sksamuel.avro4s.{FromRecord, SchemaFor, ToRecord}
import org.apache.spark.sql.{Encoder, SaveMode}
import org.elasticsearch.spark.sql._
import scala.reflect.ClassTag

class WriterSparkImpl[T] extends Writer[T] {

  override def write(ds: DataSetAPI[T]): WriterExecutor[T] = new WriterExecutor[T](ds) {

    override def apply(dataStore: CSVDataStore, saveMode: SaveMode)
                      (implicit ct: ClassTag[T], csvEncoder: class2csv.CsvEncoder[T], encoder: Encoder[T]): Unit = {

      var df = ds.toDataset.toDF
      if(dataStore.columns.nonEmpty) {
        df = df.toDF(dataStore.columns: _*)
      }

      df
        .repartition(1)
        .write
        .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
        .option("header", dataStore.header.toString)
        .option("delimiter", dataStore.delimiter.toString)
        .option("quote", dataStore.quote.toString)
        .option("escape", dataStore.escape.toString)
        //.option("charset", dataStore.charset)
        .mode(saveMode)
        .csv(dataStore.path)
    }

    override def apply(dataStore: JsonDataStore, saveMode: SaveMode)
                      (implicit encoder: Encoder[T]): Unit =
      ds
        .toDataset
        .repartition(1)
        .write
        .mode(saveMode)
        .json(dataStore.path)

    override def apply(dataStore: ParquetDataStore, saveMode: SaveMode)
                      (implicit s: SchemaFor[T], fromR: FromRecord[T], toR: ToRecord[T], encoder: Encoder[T]): Unit =
      ds
        .toDataset
        .repartition(1)
        .write
        .mode(saveMode)
        .parquet(dataStore.path)

    override def apply(dataStore: AvroDataStore, saveMode: SaveMode)
                      (implicit s: SchemaFor[T], r: ToRecord[T], encoder: Encoder[T]): Unit =
      ds
        .toDataset
        .repartition(1)
        .write
        .mode(saveMode)
        .avro(dataStore.path)

    override def apply(dataStore: HiveDataStore, saveMode: SaveMode)
                      (implicit s: SchemaFor[T], r: ToRecord[T], encoder: Encoder[T]): Unit =
      ds
        .toDataset
        .write
        .mode(saveMode)
        .saveAsTable(dataStore.fullTableName)

    override def apply(dataStore: JdbcDataStore, saveMode: SaveMode)
                      (implicit ct: ClassTag[T], jdbcEncoder: com.datawizards.class2jdbc.JdbcEncoder[T], encoder: Encoder[T]): Unit = {
      Class.forName(dataStore.driverClassName)
      ds
        .toDataset
        .write
        .mode(saveMode)
        .jdbc(dataStore.url, dataStore.fullTableName, dataStore.connectionProperties)
    }

    override protected def writeToElasticsearch(dataStore: ElasticsearchDataStore)
                                               (implicit ct: ClassTag[T], encoder: Encoder[T]): Unit =
      ds
        .toDataset
        .saveToEs(dataStore.elasticsearchResourceName, dataStore.getConfigForSparkWriter)
  }

}
