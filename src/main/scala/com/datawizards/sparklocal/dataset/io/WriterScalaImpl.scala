package com.datawizards.sparklocal.dataset.io

import java.io.PrintWriter

import com.datawizards.class2csv._
import org.json4s.jackson.Serialization
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.datastore._
import org.apache.spark.sql.SaveMode
import org.json4s.DefaultFormats

import scala.reflect.ClassTag

class WriterScalaImpl[T <: AnyRef] extends Writer[T] {

  override def write(ds: DataSetAPI[T]): WriterExecutor[T] = new WriterExecutor[T](ds) {

    override def apply(dataStore: StdoutStore, saveMode: SaveMode): Unit =
      ???

    override def apply(dataStore: CSVDataStore, saveMode: SaveMode)
                      (implicit ct: ClassTag[T], enc: CsvEncoder[T]): Unit =
      writeCSV(
        data = ds.collect(),
        path = dataStore.path,
        delimiter = dataStore.delimiter,
        header = dataStore.header,
        columns = dataStore.columns,
        escape = dataStore.escape,
        quote = dataStore.quote
      )

    override def apply(dataStore: JsonDataStore, saveMode: SaveMode): Unit = {
      implicit val formats = DefaultFormats

      val pw = new PrintWriter(dataStore.path)

      for(e <- ds) {
        pw.write(Serialization.write[T](e)(formats))
        pw.write("\n")
      }
        //Serialization.write[T, PrintWriter](e, pw)

      pw.close()
    }

    override def apply(dataStore: ParquetDataStore, saveMode: SaveMode): Unit =
      ???

    override def apply(dataStore: AvroDataStore, saveMode: SaveMode): Unit =
      ???
  }

}
