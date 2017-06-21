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
import org.apache.spark.sql.types.StructType
import shapeless.Generic.Aux
import shapeless.HList

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

object ReaderSparkImpl extends Reader {
  private lazy val spark: SparkSession = SparkSession.builder().getOrCreate()

  override def read[T]: ReaderExecutor[T] = new ReaderExecutor[T] {
    override def apply[L <: HList](dataStore: datastore.CSVDataStore)
                                  (implicit ct: ClassTag[T], tt: TypeTag[T], gen: Aux[T, L], fromRow: csv2class.FromRow[L], enc: Encoder[T]): DataSetAPI[T] = {

      val classTypeMetaData = extractClassMetaData(ModelDialects.CSV)
      var df = spark
        .read
        .option("header", dataStore.header.toString)
        .option("delimiter", dataStore.delimiter.toString)
        .option("quote", dataStore.quote.toString)
        .option("escape", dataStore.escape.toString)
        .option("parserLib", "univocity")
        .schema(buildSchema(classTypeMetaData))
        .csv(dataStore.path)

      if (!dataStore.header) {
        df = df.toDF(dataStore.columns: _*)
      }

      mapInputDataFrameToDataset(df, classTypeMetaData)
    }

    override def apply(dataStore: datastore.JsonDataStore)(implicit ct: ClassTag[T], tt: TypeTag[T]): DataSetAPI[T] = {
      val classTypeMetaData = extractClassMetaData(ModelDialects.JSON)
      implicit val enc = ExpressionEncoder[T]()
      mapInputDataFrameToDataset(
        spark
          .read
          .schema(buildSchema(classTypeMetaData))
          .json(dataStore.path),
        classTypeMetaData
      )(enc, ct, tt)
    }

    override def apply(dataStore: datastore.ParquetDataStore)
                      (implicit ct: ClassTag[T], tt: TypeTag[T], s: SchemaFor[T], fromR: FromRecord[T], toR: ToRecord[T], enc: Encoder[T]): DataSetAPI[T] = {
      val classTypeMetaData = extractClassMetaData(ModelDialects.Parquet)
      mapInputDataFrameToDataset(
        spark
          .read
          .schema(buildSchema(classTypeMetaData))
          .parquet(dataStore.path),
        classTypeMetaData
      )
    }

    override def apply(dataStore: datastore.AvroDataStore)
                      (implicit ct: ClassTag[T], tt: TypeTag[T], s: SchemaFor[T], r: FromRecord[T], enc: Encoder[T]): DataSetAPI[T] = {
      val classTypeMetaData = extractClassMetaData(ModelDialects.Avro)
      mapInputDataFrameToDataset(
        spark
          .read
          .schema(buildSchema(classTypeMetaData))
          .avro(dataStore.path),
        classTypeMetaData
      )
    }

    override def apply(dataStore: datastore.HiveDataStore)
                      (implicit ct: ClassTag[T], tt: TypeTag[T], s: SchemaFor[T], r: FromRecord[T], enc: Encoder[T]): DataSetAPI[T] = {
      val classTypeMetaData = extractClassMetaData(dialects.Hive)
      mapInputDataFrameToDataset(
        spark
          .read
          .schema(buildSchema(classTypeMetaData))
          .table(dataStore.fullTableName),
        classTypeMetaData
      )
    }

    override def apply[L <: HList](dataStore: datastore.JdbcDataStore)
                                  (implicit ct: ClassTag[T], tt: TypeTag[T], gen: Aux[T, L], fromRow: csv2class.FromRow[L], enc: Encoder[T]): DataSetAPI[T] = {
      Class.forName(dataStore.driverClassName)
      // TODO - mapInputDataFrameToDataset - map driver name to dialect???
      // create subclasses of JdbcDataStore. JdbcDataStore should be abstract in this case to force providing supported dialect!
      DataSetAPI(
        spark
          .read
          //TODO - add schema using JDBC dialect
          .jdbc(dataStore.url, dataStore.fullTableName, dataStore.connectionProperties)
          .as[T]
      )
    }

    private def buildSchema(classTypeMetaData: ClassTypeMetaData)
                           (implicit enc: Encoder[T], ct: ClassTag[T], tt: TypeTag[T]): StructType = {
      //TODO - support for nested fields
      val typeSchema = enc.schema
      val mapping = getColumnsMappingFromOriginalToAnnotation(classTypeMetaData)
      StructType(
        typeSchema
          .fields
          .map(f => f.copy(name = mapping(f.name)))
      )
    }

    private def mapInputDataFrameToDataset(input: DataFrame, classTypeMetaData: ClassTypeMetaData)
                                          (implicit enc: Encoder[T], ct: ClassTag[T], tt: TypeTag[T]): DataSetAPI[T] =
      DataSetAPI(
        mapSourceColumnsToDatasetFields(
          projectTargetColumns(input, classTypeMetaData),
          classTypeMetaData
        ).as[T]
      )

    private def extractClassMetaData(dialect: Dialect)
                                    (implicit tt: TypeTag[T]) =
      MetaDataExtractor.extractClassMetaDataForDialect[T](dialect)

    private def projectTargetColumns(df: DataFrame, classTypeMetaData: ClassTypeMetaData): DataFrame =
      projectSelectedColumns(df, classTypeMetaData.fields.map(_.fieldName).toSeq)

    private def projectSelectedColumns(df: DataFrame, columns: Seq[String]): DataFrame =
      addMissingColumnsToDf(selectCommonColumns(df, columns), columns)

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

    private def mapSourceColumnsToDatasetFields(input: DataFrame, classTypeMetaData: ClassTypeMetaData): DataFrame = {
      val columnsMapping = getColumnsMappingFromAnnotationToOriginal(classTypeMetaData)
      input.toDF(
        input.schema.fields.map(f => columnsMapping(f.name)):_*
      )
    }

    private def getColumnsMappingFromOriginalToAnnotation(classTypeMetaData: ClassTypeMetaData): Map[String, String] =
      classTypeMetaData
        .fields
        .map(f => (f.originalFieldName, f.fieldName))
        .toMap

    private def getColumnsMappingFromAnnotationToOriginal(classTypeMetaData: ClassTypeMetaData): Map[String, String] =
      classTypeMetaData
        .fields
        .map(f => (f.fieldName, f.originalFieldName))
        .toMap

  }

}
