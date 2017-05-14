package com.datawizards.sparklocal.dataset.io

import com.datawizards.class2csv
import com.databricks.spark.avro._
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.datastore._
import com.sksamuel.avro4s.{FromRecord, SchemaFor, ToRecord}
import org.apache.spark.sql.SaveMode

import scala.reflect.ClassTag

class WriterSparkImpl[T] extends Writer[T] {

  override def write(ds: DataSetAPI[T]): WriterExecutor[T] = new WriterExecutor[T](ds) {

    override def apply(dataStore: StdoutStore, saveMode: SaveMode): Unit =
      ds
        .toDataset
        .show()

    override def apply(dataStore: CSVDataStore, saveMode: SaveMode)
                      (implicit ct: ClassTag[T], enc: class2csv.CsvEncoder[T]): Unit = {

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

    override def apply(dataStore: JsonDataStore, saveMode: SaveMode): Unit =
      ds
        .toDataset
        .repartition(1)
        .write
        .mode(saveMode)
        .json(dataStore.path)

    override def apply(dataStore: ParquetDataStore, saveMode: SaveMode)
                      (implicit s: SchemaFor[T], fromR: FromRecord[T], toR: ToRecord[T]): Unit =
      ds
        .toDataset
        .repartition(1)
        .write
        .mode(saveMode)
        .parquet(dataStore.path)

    override def apply(dataStore: AvroDataStore, saveMode: SaveMode)
                      (implicit s: SchemaFor[T], r: ToRecord[T]): Unit =
      ds
        .toDataset
        .repartition(1)
        .write
        .mode(saveMode)
        .avro(dataStore.path)
  }

}
