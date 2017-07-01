package com.datawizards.sparklocal.impl.scala.dataset.io

import java.io.{File, PrintWriter}
import java.nio.file.Files
import java.sql.DriverManager

import com.datawizards.class2csv._
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.dataset.io._
import com.datawizards.sparklocal.datastore._
import com.datawizards.class2jdbc._
import com.datawizards.dmg.dialects.{Dialect, HiveDialect}
import com.datawizards.dmg.metadata.MetaDataExtractor
import com.datawizards.esclient.repository.ElasticsearchRepositoryImpl
import com.sksamuel.avro4s._
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.spark.sql.{Encoder, SaveMode}
import org.json4s.JsonAST.JObject
import org.json4s.{DefaultFormats, Extraction, JValue}
import org.json4s.jackson.JsonMethods

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
          columns = if(dataStore.columns.nonEmpty) dataStore.columns else extractTargetColumns(ModelDialects.CSV),
          escape = dataStore.escape,
          quote = dataStore.quote
        )
      }

    override def apply(dataStore: JsonDataStore, saveMode: SaveMode)
                      (implicit encoder: Encoder[T], tt: TypeTag[T]): Unit = {
      genericFileWrite(dataStore, saveMode) { file =>
        implicit val formats = DefaultFormats
        val pw = new PrintWriter(file)
        for (e <- ds) {
          e match {
            case a: AnyRef =>
              val json = Extraction.decompose(a)(formats)
              val mappedJson = FieldNamesMappingUtils.changeJsonObjectFieldNames[T](json, true)
              pw.write(JsonMethods.mapper.writeValueAsString(mappedJson))
            case _ => throw new Exception("Not supported type for JSON serialization!")
          }
          pw.write("\n")
        }
        pw.close()
      }
    }

    override def apply(dataStore: ParquetDataStore, saveMode: SaveMode)
                      (implicit tt: TypeTag[T], s: SchemaFor[T], fromR: FromRecord[T], toR: ToRecord[T], encoder: Encoder[T]): Unit =
      genericFileWrite(dataStore, saveMode) {file =>
        val fieldNameMapping = FieldNamesMappingUtils.constructFieldNameMapping(ParquetDialect)
        val mappedSchema = AvroUtils.mapSchema(s(), fieldNameMapping)
        val writer = AvroParquetWriter
          .builder[GenericRecord](new Path(file.getPath))
          .withSchema(mappedSchema)
          .build()
        val format = RecordFormat[T]
        for(e <- ds)
          writer.write(AvroUtils.mapGenericRecordFromOriginalToTarget(format.to(e), mappedSchema, fieldNameMapping))
        writer.close()
      }

    override def apply(dataStore: AvroDataStore, saveMode: SaveMode)
                      (implicit tt: TypeTag[T], s: SchemaFor[T], r: ToRecord[T], encoder: Encoder[T]): Unit =
      writeAvro(dataStore, saveMode, AvroDialect)

    override def apply(dataStore: HiveDataStore, saveMode: SaveMode)
                      (implicit tt: TypeTag[T], s: SchemaFor[T], r: ToRecord[T], encoder: Encoder[T]): Unit = {
      val file = new File(dataStore.localDirectoryPath)
      file.mkdirs()
      writeAvro(AvroDataStore(dataStore.localFilePath), saveMode, HiveDialect)
    }

    override protected def writeToJdbc(dataStore: JdbcDataStore)
                      (implicit ct: ClassTag[T], tt: TypeTag[T], jdbcEncoder: com.datawizards.class2jdbc.JdbcEncoder[T], encoder: Encoder[T]): Unit = {
      Class.forName(dataStore.driverClassName)
      val connection = DriverManager.getConnection(dataStore.url, dataStore.connectionProperties)
      val classTypeMetaData = MetaDataExtractor.extractClassMetaDataForDialect[T](dataStore.dialect)
      val inserts = generateInserts(ds.collect(), dataStore.fullTableName, classTypeMetaData.fields.map(f => f.fieldName).toSeq)
      connection.createStatement().execute(inserts.mkString(";"))
      connection.close()
    }

    private def writeAvro(dataStore: AvroDataStore, saveMode: SaveMode, dialect: Dialect)
                         (implicit tt: TypeTag[T], s: SchemaFor[T], r: ToRecord[T], encoder: Encoder[T]): Unit = {
      val fieldNameMapping = FieldNamesMappingUtils.constructFieldNameMapping(dialect)
      val mappedSchema = AvroUtils.mapSchema(s(), fieldNameMapping)

      genericFileWrite(dataStore, saveMode) { file =>
        val datumWriter = new GenericDatumWriter[GenericRecord](mappedSchema)
        val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
        val os = Files.newOutputStream(file.toPath)
        dataFileWriter.create(mappedSchema, os)
        val toRecord = implicitly[ToRecord[T]]
        ds
          .collect()
          .map(o => toRecord.apply(o))
          .map(r => AvroUtils.mapGenericRecordFromOriginalToTarget(r, mappedSchema, fieldNameMapping))
          .foreach(r => dataFileWriter.append(r))

        dataFileWriter.flush()
        dataFileWriter.close()
        os.close()
      }
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
