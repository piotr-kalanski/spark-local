package com.datawizards.sparklocal.dataset.io

import java.io.File

import com.datawizards.csv2class
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.datastore

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import com.datawizards.csv2class._
import shapeless.Generic.Aux
import shapeless.HList
import org.json4s._
import org.json4s.native.JsonMethods._
import com.sksamuel.avro4s._
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetReader
import org.apache.spark.sql.Encoder

object ReaderScalaImpl extends Reader {

  override def read[T]: ReaderExecutor[T] = new ReaderExecutor[T] {
    override def apply[L <: HList](dataStore: datastore.CSVDataStore)
                                  (implicit ct: ClassTag[T], gen: Aux[T, L], fromRow: csv2class.FromRow[L], enc: Encoder[T]): DataSetAPI[T] = {
      val parsed = parseCSV[T](
        path = dataStore.path,
        delimiter = dataStore.delimiter,
        quote = dataStore.quote,
        escape = dataStore.escape,
        header = dataStore.header,
        columns = dataStore.columns
      )

      DataSetAPI(parsed._1)
    }

    override def apply(dataStore: datastore.JsonDataStore)(implicit ct: ClassTag[T], tt: TypeTag[T]): DataSetAPI[T] = {
      implicit val formats = DefaultFormats

      DataSetAPI(
        scala.io.Source
          .fromFile(dataStore.path)
          .getLines()
          .map(line => parse(line).extract[T])
          .toIterable
      )
    }

    override def apply(dataStore: datastore.ParquetDataStore)
                      (implicit ct: ClassTag[T], s: SchemaFor[T], fromR: FromRecord[T], toR: ToRecord[T], enc: Encoder[T]): DataSetAPI[T] = {
      val reader = AvroParquetReader.builder[GenericRecord](new Path(dataStore.path)).build()
      val format = RecordFormat[T]
      val iterator = Iterator.continually(reader.read).takeWhile(_ != null).map(format.from)
      DataSetAPI(iterator.toStream)
    }

    override def apply(dataStore: datastore.AvroDataStore)
                      (implicit ct: ClassTag[T], s: SchemaFor[T], r: FromRecord[T], enc: Encoder[T]): DataSetAPI[T] = {
      val is = AvroInputStream.data[T](new File(dataStore.path))
      val data = is.iterator.toSet
      is.close()
      DataSetAPI(data)
    }

    override def apply(dataStore: datastore.HiveDataStore)
                      (implicit ct: ClassTag[T], s: SchemaFor[T], r: FromRecord[T], enc: Encoder[T]): DataSetAPI[T] =
      apply(datastore.AvroDataStore(dataStore.localFilePath))

  }

}
