package com.datawizards.sparklocal.dataset.io

import java.io.{File, PrintWriter}
import java.sql.DriverManager

import com.datawizards.class2csv._
import com.datawizards.sparklocal.dataset.io.class2jdbc._
import org.json4s.jackson.Serialization
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.datastore._
import org.apache.spark.sql.{Encoder, SaveMode}
import org.json4s.DefaultFormats
import com.sksamuel.avro4s._
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter

import scala.reflect.ClassTag

class WriterScalaImpl[T] extends Writer[T] {

  override def write(ds: DataSetAPI[T]): WriterExecutor[T] = new WriterExecutor[T](ds) {

    override def apply(dataStore: CSVDataStore, saveMode: SaveMode)
                      (implicit ct: ClassTag[T], csvEncoder: CsvEncoder[T], encoder: Encoder[T]): Unit =
      writeCSV(
        data = ds.collect(),
        path = dataStore.path,
        delimiter = dataStore.delimiter,
        header = dataStore.header,
        columns = dataStore.columns,
        escape = dataStore.escape,
        quote = dataStore.quote
      )

    override def apply(dataStore: JsonDataStore, saveMode: SaveMode)
                      (implicit encoder: Encoder[T]): Unit = {
      implicit val formats = DefaultFormats

      val pw = new PrintWriter(dataStore.path)

      for(e <- ds) {
        e match {
          case a:AnyRef => pw.write(Serialization.write(a)(formats))
          case _ => throw new Exception("Not supported type for JSON serialization!")
        }
        pw.write("\n")
      }

      pw.close()
    }

    override def apply(dataStore: ParquetDataStore, saveMode: SaveMode)
                      (implicit s: SchemaFor[T], fromR: FromRecord[T], toR: ToRecord[T], encoder: Encoder[T]): Unit = {
      val file = new File(dataStore.path)
      //TODO - delete only in overwrite mode!
      file.delete()
      val writer = AvroParquetWriter
          .builder[GenericRecord](new Path(dataStore.path))
          .withSchema(s())
          .build()
      val format = RecordFormat[T]
      for(e <- ds)
        writer.write(format.to(e))
      writer.close()
    }

    override def apply(dataStore: AvroDataStore, saveMode: SaveMode)
                      (implicit s: SchemaFor[T], r: ToRecord[T], encoder: Encoder[T]): Unit = {
      val os = AvroOutputStream.data[T](new File(dataStore.path))
      os.write(ds.collect())
      os.flush()
      os.close()
    }

    override def apply(dataStore: HiveDataStore, saveMode: SaveMode)
                      (implicit s: SchemaFor[T], r: ToRecord[T], encoder: Encoder[T]): Unit = {
      val file = new File(dataStore.localDirectoryPath)
      file.mkdirs()
      apply(AvroDataStore(dataStore.localFilePath), saveMode)
    }

    override def apply(dataStore: JdbcDataStore, saveMode: SaveMode)
                      (implicit ct: ClassTag[T], jdbcEncoder: class2jdbc.JdbcEncoder[T], encoder: Encoder[T]): Unit = {
      Class.forName(dataStore.driverClassName)
      val connection = DriverManager.getConnection(dataStore.url, dataStore.connectionProperties)
      val inserts = generateInserts(ds.collect(), dataStore.fullTableName)
      connection.createStatement().execute(inserts.mkString(";"))
      connection.close()
    }
  }

}
