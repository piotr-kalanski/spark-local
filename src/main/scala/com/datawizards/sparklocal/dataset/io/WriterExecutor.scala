package com.datawizards.sparklocal.dataset.io

import com.datawizards.class2csv._
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.class2jdbc._
import com.datawizards.dmg.dialects.Dialect
import com.datawizards.dmg.metadata.MetaDataExtractor
import com.datawizards.esclient.repository.ElasticsearchRepositoryImpl
import com.datawizards.sparklocal.datastore._
import com.sksamuel.avro4s.{FromRecord, SchemaFor, ToRecord}
import org.apache.spark.sql.{Encoder, SaveMode}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

abstract class WriterExecutor[T](ds: DataSetAPI[T]) {
  def apply(dataStore: CSVDataStore, saveMode: SaveMode)
           (implicit ct: ClassTag[T], tt: TypeTag[T], csvEncoder: CsvEncoder[T], encoder: Encoder[T]): Unit
  def apply(dataStore: JsonDataStore, saveMode: SaveMode)
           (implicit encoder: Encoder[T], tt: TypeTag[T]): Unit
  def apply(dataStore: ParquetDataStore, saveMode: SaveMode)
           (implicit tt: TypeTag[T], s: SchemaFor[T], fromR: FromRecord[T], toR: ToRecord[T], encoder: Encoder[T]): Unit
  def apply(dataStore: AvroDataStore, saveMode: SaveMode)
           (implicit tt: TypeTag[T], s: SchemaFor[T], r: ToRecord[T], encoder: Encoder[T]): Unit
  def apply(dataStore: HiveDataStore, saveMode: SaveMode)
           (implicit tt: TypeTag[T], s: SchemaFor[T], r: ToRecord[T], encoder: Encoder[T]): Unit
  def apply(dataStore: JdbcDataStore, saveMode: SaveMode)
           (implicit ct: ClassTag[T], tt: TypeTag[T], jdbcEncoder: JdbcEncoder[T], encoder: Encoder[T]): Unit
  def apply()(implicit ct: ClassTag[T], tt: TypeTag[T], csvEncoder: CsvEncoder[T]): String =
    apply(Stdout())
  def apply(rows:Int)(implicit ct: ClassTag[T], tt: TypeTag[T], csvEncoder: CsvEncoder[T]): String =
    apply(Stdout(rows))
  def apply(dataStore: Stdout, saveMode: SaveMode)
           (implicit ct: ClassTag[T], tt: TypeTag[T], csvEncoder: CsvEncoder[T]): String =
    apply(dataStore)
  def apply(dataStore: Stdout)
           (implicit ct: ClassTag[T], tt: TypeTag[T], csvEncoder: CsvEncoder[T]): String = {
    val sep = "|"
    val encodedRows = ds.take(dataStore.rows).map(r => csvEncoder.encode(r))
    val classFields = ct.runtimeClass.getDeclaredFields.map(_.getName)
    val fields:Array[String] =
      if(classFields.nonEmpty) classFields
      else encodedRows.head.zipWithIndex.map(p => "_" + (p._2+1).toString).toArray
    val buffer = new StringBuilder

    def calculateColumnsLengths(): Map[Int, Int] = {
      encodedRows
        .flatMap(r => r.zipWithIndex.map(p => (p._2, p._1.length))) // column number -> column length
        .union(fields.zipWithIndex.map(p => (p._2, p._1.length)))
        .groupBy(_._1)
        .mapValues(vals => vals.maxBy(_._2)._2)
    }

    def calculateHorizontalSeparator(columnsLengths: Map[Int, Int]): String =
      "+" + columnsLengths.toSeq.sorted.map(_._2).map(v => "-" * v).mkString("+") + "+\n"

    def generateHeader(columnsLengths: Map[Int,Int], horizontalSeparator: String): Unit = {
      buffer ++= horizontalSeparator
      var i = 0
      buffer ++= sep
      for(f <- fields) {
        buffer ++= f.toString.padTo(columnsLengths(i), " ").mkString("")
        buffer ++= sep
        i += 1
      }
      buffer ++= "\n"
      buffer ++= horizontalSeparator
    }

    def generateRows(columnsLengths: Map[Int,Int], horizontalSeparator: String): Unit = {
      for(r <- encodedRows) {
        buffer ++= sep
        for((f,i) <- r.zipWithIndex) {
          buffer ++= f.padTo(columnsLengths(i), " ").mkString("")
          buffer ++= sep
        }
        buffer ++= "\n"
      }
      buffer ++= horizontalSeparator
    }

    val columnsLengths: Map[Int,Int] = calculateColumnsLengths()
    val hSeparator = calculateHorizontalSeparator(columnsLengths)
    generateHeader(columnsLengths, hSeparator)
    generateRows(columnsLengths, hSeparator)

    val result = buffer.toString
    println(result)
    result
  }
  def apply(dataStore: ElasticsearchDataStore, saveMode: SaveMode)
           (implicit ct: ClassTag[T], tt: TypeTag[T], encoder: Encoder[T]): Unit = {
    val repository = new ElasticsearchRepositoryImpl(dataStore.getRestAPIURL)
    saveMode match {
      case SaveMode.Append =>
        writeToElasticsearch(dataStore)
      case SaveMode.ErrorIfExists =>
        if(repository.indexExists(dataStore.elasticsearchIndexName))
          throw new Exception("Index exists: " + dataStore.elasticsearchIndexName)
        else
          writeToElasticsearch(dataStore)
      case SaveMode.Overwrite =>
        repository.deleteIndexIfNotExists(dataStore.elasticsearchIndexName)
        writeToElasticsearch(dataStore)
      case SaveMode.Ignore =>
        if(!repository.indexExists(dataStore.elasticsearchIndexName))
          writeToElasticsearch(dataStore)
    }
  }

  def apply(dataStore: DataStore, saveMode: SaveMode)
           (implicit
            ct: ClassTag[T],
            tt: TypeTag[T],
            csvEncoder: CsvEncoder[T],
            s: SchemaFor[T],
            fromR: FromRecord[T],
            toR: ToRecord[T],
            jdbcEncoder: JdbcEncoder[T],
            encoder: Encoder[T]
           ): Unit = dataStore match {
    case d:CSVDataStore => this.apply(d, saveMode)
    case d:JsonDataStore => this.apply(d, saveMode)
    case d:ParquetDataStore => this.apply(d, saveMode)
    case d:AvroDataStore => this.apply(d, saveMode)
    case d:JdbcDataStore => this.apply(d, saveMode)
    case d:ElasticsearchDataStore => this.apply(d, saveMode)
    case d:HiveDataStore => this.apply(d, saveMode)
    case d:Stdout => this.apply(d)
    case _ => throw new IllegalArgumentException("Not supported: " + dataStore)
  }

  def csv(dataStore: CSVDataStore, saveMode: SaveMode)
           (implicit ct: ClassTag[T], tt: TypeTag[T], csvEncoder: CsvEncoder[T], encoder: Encoder[T]): Unit =
    this.apply(dataStore, saveMode)
  def json(dataStore: JsonDataStore, saveMode: SaveMode)
           (implicit tt: TypeTag[T], encoder: Encoder[T]): Unit =
    this.apply(dataStore, saveMode)
  def parquet(dataStore: ParquetDataStore, saveMode: SaveMode)
           (implicit tt: TypeTag[T], s: SchemaFor[T], fromR: FromRecord[T], toR: ToRecord[T], encoder: Encoder[T]): Unit =
    this.apply(dataStore, saveMode)
  def avro(dataStore: AvroDataStore, saveMode: SaveMode)
           (implicit tt: TypeTag[T], s: SchemaFor[T], r: ToRecord[T], encoder: Encoder[T]): Unit =
    this.apply(dataStore, saveMode)
  def table(dataStore: HiveDataStore, saveMode: SaveMode)
           (implicit tt: TypeTag[T], s: SchemaFor[T], r: ToRecord[T], encoder: Encoder[T]): Unit =
    this.apply(dataStore, saveMode)
  def jdbc(dataStore: JdbcDataStore, saveMode: SaveMode)
           (implicit ct: ClassTag[T], tt: TypeTag[T], jdbcEncoder: JdbcEncoder[T], encoder: Encoder[T]): Unit =
    this.apply(dataStore, saveMode)
  def es(dataStore: ElasticsearchDataStore, saveMode: SaveMode)
        (implicit ct: ClassTag[T], tt: TypeTag[T], encoder: Encoder[T]): Unit =
    this.apply(dataStore, saveMode)

  protected def writeToElasticsearch(dataStore: ElasticsearchDataStore)(implicit ct: ClassTag[T], tt: TypeTag[T], encoder: Encoder[T]): Unit

  protected def extractTargetColumns(dialect: Dialect)
                                  (implicit tt: TypeTag[T]): Seq[String] = {
    val classTypeMetaData = MetaDataExtractor.extractClassMetaDataForDialect[T](dialect)
    classTypeMetaData.fields.map(_.fieldName).toSeq
  }
}
