package com.datawizards.sparklocal.impl.spark.dataset.io

import com.databricks.spark.avro._
import com.datawizards.csv2class
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.dataset.io.{Reader, ReaderExecutor}
import com.datawizards.sparklocal.datastore
import com.sksamuel.avro4s.{FromRecord, SchemaFor, ToRecord}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Encoder, SparkSession}
import shapeless.Generic.Aux
import shapeless.HList

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

object ReaderSparkImpl extends Reader {
  private lazy val spark: SparkSession = SparkSession.builder().getOrCreate()

  override def read[T]: ReaderExecutor[T] = new ReaderExecutor[T] {
    override def apply[L <: HList](dataStore: datastore.CSVDataStore)
                                  (implicit ct: ClassTag[T], gen: Aux[T, L], fromRow: csv2class.FromRow[L], enc: Encoder[T]): DataSetAPI[T] = {
      var df = spark
        .read
        .option("header", dataStore.header.toString)
        .option("delimiter", dataStore.delimiter.toString)
        .option("quote", dataStore.quote.toString)
        .option("escape", dataStore.escape.toString)
        .option("parserLib", "univocity")
        .schema(enc.schema)
        //.option("charset", dataStore.charset)
        .csv(dataStore.path)

      if(!dataStore.header) {
        df = df.toDF(dataStore.columns: _*)
      }

      DataSetAPI(df.as[T])
    }

    override def apply(dataStore: datastore.JsonDataStore)(implicit ct: ClassTag[T], tt: TypeTag[T]): DataSetAPI[T] = {
      val enc = ExpressionEncoder[T]()
      DataSetAPI(
        spark
          .read
          .schema(enc.schema)
          .json(dataStore.path)
          .as[T](enc)
      )
    }

    override def apply(dataStore: datastore.ParquetDataStore)
                      (implicit ct: ClassTag[T], s: SchemaFor[T], fromR: FromRecord[T], toR: ToRecord[T], enc: Encoder[T]): DataSetAPI[T] =
      DataSetAPI(
        spark
          .read
          .schema(enc.schema)
          .parquet(dataStore.path)
          .as[T]
      )

    override def apply(dataStore: datastore.AvroDataStore)
                      (implicit ct: ClassTag[T], s: SchemaFor[T], r: FromRecord[T], enc: Encoder[T]): DataSetAPI[T] =
      DataSetAPI(
        spark
          .read
          .schema(enc.schema)
          .avro(dataStore.path)
          .as[T]
      )

    override def apply(dataStore: datastore.HiveDataStore)
                      (implicit ct: ClassTag[T], s: SchemaFor[T], r: FromRecord[T], enc: Encoder[T]): DataSetAPI[T] =
      DataSetAPI(
        spark
          .read
          .table(dataStore.fullTableName)
          .as[T]
      )

    override def apply[L <: HList](dataStore: datastore.JdbcDataStore)
                                  (implicit ct: ClassTag[T], gen: Aux[T, L], fromRow: csv2class.FromRow[L], enc: Encoder[T]): DataSetAPI[T] = {
      Class.forName(dataStore.driverClassName)
      DataSetAPI(
        spark
          .read
          .jdbc(dataStore.url, dataStore.fullTableName, dataStore.connectionProperties)
          .as[T]
      )
    }
  }

}
