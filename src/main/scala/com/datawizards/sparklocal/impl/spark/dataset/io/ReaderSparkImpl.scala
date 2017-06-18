package com.datawizards.sparklocal.impl.spark.dataset.io

import com.databricks.spark.avro._
import com.datawizards.csv2class
import com.datawizards.dmg.dialects
import com.datawizards.dmg.dialects.Dialect
import com.datawizards.dmg.metadata.{ClassTypeMetaData, MetaDataExtractor}
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.dataset.io.{Reader, ReaderExecutor}
import com.datawizards.sparklocal.dataset.io.ModelDialects
import com.datawizards.sparklocal.datastore
import com.sksamuel.avro4s.{FromRecord, SchemaFor, ToRecord}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{DataFrame, Encoder, SparkSession}
import org.apache.spark.sql.functions.lit
import shapeless.Generic.Aux
import shapeless.HList

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

object ReaderSparkImpl extends Reader {
  private lazy val spark: SparkSession = SparkSession.builder().getOrCreate()

  override def read[T]: ReaderExecutor[T] = new ReaderExecutor[T] {
    override def apply[L <: HList](dataStore: datastore.CSVDataStore)
                                  (implicit ct: ClassTag[T], tt: TypeTag[T], gen: Aux[T, L], fromRow: csv2class.FromRow[L], enc: Encoder[T]): DataSetAPI[T] = {
      var df = spark
        .read
        .option("header", dataStore.header.toString)
        .option("delimiter", dataStore.delimiter.toString)
        .option("quote", dataStore.quote.toString)
        .option("escape", dataStore.escape.toString)
        .option("parserLib", "univocity")
        .schema(enc.schema)
        .csv(dataStore.path)

      if(!dataStore.header) {
        df = df.toDF(dataStore.columns: _*)
      }

      mapInputDataFrameToDataset(df, ModelDialects.CSV)
    }

    override def apply(dataStore: datastore.JsonDataStore)(implicit ct: ClassTag[T], tt: TypeTag[T]): DataSetAPI[T] = {
      val enc = ExpressionEncoder[T]()
      mapInputDataFrameToDataset(
        spark
          .read
          .schema(enc.schema)
          .json(dataStore.path),
        ModelDialects.JSON
      )(enc, ct, tt)

    }

    override def apply(dataStore: datastore.ParquetDataStore)
                      (implicit ct: ClassTag[T], tt: TypeTag[T], s: SchemaFor[T], fromR: FromRecord[T], toR: ToRecord[T], enc: Encoder[T]): DataSetAPI[T] =
      mapInputDataFrameToDataset(
        spark
          .read
          .schema(enc.schema)
          .parquet(dataStore.path),
        ModelDialects.Parquet
      )

    override def apply(dataStore: datastore.AvroDataStore)
                      (implicit ct: ClassTag[T], tt: TypeTag[T], s: SchemaFor[T], r: FromRecord[T], enc: Encoder[T]): DataSetAPI[T] =
      mapInputDataFrameToDataset(
        spark
          .read
          .schema(enc.schema)
          .avro(dataStore.path),
        ModelDialects.Avro
      )

    override def apply(dataStore: datastore.HiveDataStore)
                      (implicit ct: ClassTag[T], tt: TypeTag[T], s: SchemaFor[T], r: FromRecord[T], enc: Encoder[T]): DataSetAPI[T] =
      mapInputDataFrameToDataset(
        spark
          .read
          .table(dataStore.fullTableName),
        dialects.Hive
      )

    override def apply[L <: HList](dataStore: datastore.JdbcDataStore)
                                  (implicit ct: ClassTag[T], tt: TypeTag[T], gen: Aux[T, L], fromRow: csv2class.FromRow[L], enc: Encoder[T]): DataSetAPI[T] = {
      Class.forName(dataStore.driverClassName)
      //TODO - mapInputDataFrameToDataset - map driver name to dialect???
      DataSetAPI(
        spark
          .read
          .jdbc(dataStore.url, dataStore.fullTableName, dataStore.connectionProperties)
          .as[T]
      )
    }

    private def mapInputDataFrameToDataset(input: DataFrame, dialect: Dialect)
                                          (implicit enc: Encoder[T], ct: ClassTag[T], tt: TypeTag[T]): DataSetAPI[T] = {
      val classTypeMetaData = extractClassMetaData(dialect)
      DataSetAPI(
        projectTargetColumns(
          mapInputColumnsToDatasetColumns(input, classTypeMetaData),
          classTypeMetaData
        ).as[T]
      )
    }

    private def extractClassMetaData(dialect: Dialect)
                                    (implicit tt: TypeTag[T]) =
      MetaDataExtractor.extractClassMetaDataForDialect[T](dialect)

    private def projectTargetColumns(df: DataFrame, classTypeMetaData: ClassTypeMetaData): DataFrame =
      projectSelectedColumns(df, classTypeMetaData.fields.map(_.fieldName).toSeq)

    private def projectSelectedColumns(df: DataFrame, columns: Seq[String]): DataFrame = {
      addMissingColumnsToDf(selectCommonColumns(df, columns), columns)
    }

    private def selectCommonColumns(df: DataFrame, columns: Seq[String]): DataFrame = {
      val commonColumns = df.columns.intersect(columns)
      df.select(commonColumns.head, commonColumns.tail: _*)
    }

    private def addMissingColumnsToDf(df: DataFrame, columns: Seq[String]): DataFrame = {
      val missingColumns = columns.diff(df.columns)
      var result = df
      for(c <- missingColumns)
        result = result.withColumn(c, lit(null))
      result
    }

    private def mapInputColumnsToDatasetColumns(input: DataFrame, classTypeMetaData: ClassTypeMetaData): DataFrame = {
      val columnsMapping = getColumnsMapping(classTypeMetaData)
      mapColumns(input, columnsMapping.values, columnsMapping.keys)
    }

    private def getColumnsMapping(classTypeMetaData: ClassTypeMetaData): Map[String, String] =
      classTypeMetaData
        .fields
        .map(f => (f.originalFieldName, f.fieldName))
        .toMap

    private def mapColumns(input: DataFrame, srcColumns: Iterable[String], dstColumns: Iterable[String]): DataFrame =
      if(srcColumns.isEmpty) input
      else mapColumns(input.withColumnRenamed(srcColumns.head, dstColumns.head), srcColumns.tail, dstColumns.tail)

  }

}
