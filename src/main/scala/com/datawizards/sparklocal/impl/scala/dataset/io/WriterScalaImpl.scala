package com.datawizards.sparklocal.impl.scala.dataset.io

import java.io.{File, PrintWriter}
import java.sql.DriverManager

import com.datawizards.class2csv._
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.dataset.io.{Writer, WriterExecutor}
import com.datawizards.sparklocal.datastore._
import com.datawizards.class2jdbc._
import com.datawizards.esclient.repository.ElasticsearchRepositoryImpl
import com.sksamuel.avro4s._
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.spark.sql.{Encoder, SaveMode}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

class WriterScalaImpl[T] extends Writer[T] {

  override def write(ds: DataSetAPI[T]): WriterExecutor[T] = new WriterExecutor[T](ds) {

    override def apply(dataStore: CSVDataStore, saveMode: SaveMode)
                      (implicit ct: ClassTag[T], tt: TypeTag[T], csvEncoder: CsvEncoder[T], encoder: Encoder[T]): Unit =
      genericFileWrite(dataStore, saveMode) {file =>
        writeCSV(
          data = ds.collect(),
          path = file.getPath,
          delimiter = dataStore.delimiter,
          header = dataStore.header,
          columns = dataStore.columns,
          escape = dataStore.escape,
          quote = dataStore.quote
        )
      }

    override def apply(dataStore: JsonDataStore, saveMode: SaveMode)
                      (implicit encoder: Encoder[T], tt: TypeTag[T]): Unit =
      genericFileWrite(dataStore, saveMode) {file =>
        implicit val formats = DefaultFormats
        val pw = new PrintWriter(file)
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
                      (implicit tt: TypeTag[T], s: SchemaFor[T], fromR: FromRecord[T], toR: ToRecord[T], encoder: Encoder[T]): Unit =
      genericFileWrite(dataStore, saveMode) {file =>
        val writer = AvroParquetWriter
          .builder[GenericRecord](new Path(file.getPath))
          .withSchema(s())
          .build()
        val format = RecordFormat[T]
        for(e <- ds)
          writer.write(format.to(e))
        writer.close()
      }

    override def apply(dataStore: AvroDataStore, saveMode: SaveMode)
                      (implicit tt: TypeTag[T], s: SchemaFor[T], r: ToRecord[T], encoder: Encoder[T]): Unit =
      genericFileWrite(dataStore, saveMode) {file =>
        val os = AvroOutputStream.data[T](file)
        os.write(ds.collect())
        os.flush()
        os.close()
      }

    override def apply(dataStore: HiveDataStore, saveMode: SaveMode)
                      (implicit tt: TypeTag[T], s: SchemaFor[T], r: ToRecord[T], encoder: Encoder[T]): Unit = {
      val file = new File(dataStore.localDirectoryPath)
      file.mkdirs()
      apply(AvroDataStore(dataStore.localFilePath), saveMode)
    }

    override def apply(dataStore: JdbcDataStore, saveMode: SaveMode)
                      (implicit ct: ClassTag[T], tt: TypeTag[T], jdbcEncoder: com.datawizards.class2jdbc.JdbcEncoder[T], encoder: Encoder[T]): Unit = {
      Class.forName(dataStore.driverClassName)
      val connection = DriverManager.getConnection(dataStore.url, dataStore.connectionProperties)
      val inserts = generateInserts(ds.collect(), dataStore.fullTableName)
      connection.createStatement().execute(inserts.mkString(";"))
      connection.close()
    }

    private def genericFileWrite(dataStore: FileDataStore, saveMode: SaveMode)
                                (writeTo: File => Unit): Unit = {
      val directory = new File(dataStore.path)
      saveMode match {
        case SaveMode.Append =>
          writeToDirectory(directory)
        case SaveMode.ErrorIfExists =>
          if(directory.exists())
            throw new Exception(s"Directory ${directory.getPath} already exists!")
          else
            writeToDirectory(directory)
        case SaveMode.Overwrite =>
          if(directory.exists())
            deleteRecursively(directory)
          writeToDirectory(directory)
        case SaveMode.Ignore =>
          if(!directory.exists())
            writeToDirectory(directory)
      }

      def writeToDirectory(directory: File): Unit = {
        if(!directory.exists())
          directory.mkdir()
        val file = new File(directory, uuid + dataStore.extension)
        writeTo(file)
      }
    }

    private def deleteRecursively(file: File): Unit = {
      if (file.isDirectory)
        file.listFiles.foreach(deleteRecursively)
      if (file.exists && !file.delete)
        throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
    }

    private def uuid: String = java.util.UUID.randomUUID.toString

    override protected def writeToElasticsearch(dataStore: ElasticsearchDataStore)
                                               (implicit ct: ClassTag[T], tt: TypeTag[T], encoder: Encoder[T]): Unit = {
      val repository = new ElasticsearchRepositoryImpl(dataStore.getRestAPIURL)
      for(e <- ds) {
        e match {
          case a:AnyRef =>
            repository.append(dataStore.elasticsearchIndexName, dataStore.elasticsearchTypeName, a)
          case _ => throw new Exception("Not supported type for elasticsearch write!")
        }
      }
    }

  }

}
