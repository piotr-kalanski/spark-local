package com.datawizards.sparklocal.impl.scala.dataset.io

import java.io.File

import com.sksamuel.avro4s.{AvroInputStream, FromRecord, SchemaFor}
import org.apache.avro.file.{DataFileReader, SeekableFileInput, SeekableInput}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}

import scala.util.Try

object AvroDataInputStream2 {
  def data[T: SchemaFor : FromRecord](file: File): AvroDataInputStream2[T] = new AvroDataInputStream2[T](new SeekableFileInput(file))
}

class AvroDataInputStream2[T](in: SeekableInput)
                         (implicit schemaFor: SchemaFor[T], fromRecord: FromRecord[T])
  extends AvroInputStream[T] {

  val datumReader = new GenericDatumReader[GenericRecord]()
  val dataFileReader = new DataFileReader[GenericRecord](in, datumReader)

  override def iterator: Iterator[T] = new Iterator[T] {
    override def hasNext: Boolean = dataFileReader.hasNext
    override def next(): T = fromRecord(dataFileReader.next)
  }

  override def tryIterator: Iterator[Try[T]] = new Iterator[Try[T]] {
    override def hasNext: Boolean = dataFileReader.hasNext
    override def next(): Try[T] = Try(fromRecord(dataFileReader.next))
  }

  override def close(): Unit = in.close()
}
