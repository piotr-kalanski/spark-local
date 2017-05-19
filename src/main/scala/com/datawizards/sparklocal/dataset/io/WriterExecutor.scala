package com.datawizards.sparklocal.dataset.io

import com.datawizards.class2csv.CsvEncoder
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.datastore._
import com.sksamuel.avro4s.{FromRecord, SchemaFor, ToRecord}
import org.apache.spark.sql.{Encoder, SaveMode}

import scala.reflect.ClassTag

abstract class WriterExecutor[T](ds: DataSetAPI[T]) {
  def apply(dataStore: CSVDataStore, saveMode: SaveMode)
           (implicit ct: ClassTag[T], csvEncoder: CsvEncoder[T], encoder: Encoder[T]): Unit
  def apply(dataStore: JsonDataStore, saveMode: SaveMode)
           (implicit encoder: Encoder[T]): Unit
  def apply(dataStore: ParquetDataStore, saveMode: SaveMode)
           (implicit s: SchemaFor[T], fromR: FromRecord[T], toR: ToRecord[T], encoder: Encoder[T]): Unit
  def apply(dataStore: AvroDataStore, saveMode: SaveMode)
           (implicit s: SchemaFor[T], r: ToRecord[T], encoder: Encoder[T]): Unit
  def apply(dataStore: HiveDataStore, saveMode: SaveMode)
           (implicit s: SchemaFor[T], r: ToRecord[T], encoder: Encoder[T]): Unit
  def apply()(implicit ct: ClassTag[T], csvEncoder: CsvEncoder[T]): String =
    apply(Stdout())
  def apply(rows:Int)(implicit ct: ClassTag[T], csvEncoder: CsvEncoder[T]): String =
    apply(Stdout(rows))
  def apply(dataStore: Stdout, saveMode: SaveMode)
           (implicit ct: ClassTag[T], csvEncoder: CsvEncoder[T]): String =
    apply(dataStore)
  def apply(dataStore: Stdout)
           (implicit ct: ClassTag[T], csvEncoder: CsvEncoder[T]): String = {
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
}
