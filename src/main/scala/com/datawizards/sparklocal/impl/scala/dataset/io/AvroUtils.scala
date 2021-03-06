package com.datawizards.sparklocal.impl.scala.dataset.io

import org.apache.avro.generic.GenericRecord
import org.apache.avro.{Schema, SchemaBuilder}

import scala.collection.JavaConversions._

object AvroUtils {

  def mapSchema(originalSchema: Schema, fieldNameMapping: Map[String, String]): Schema = {
    var customSchemaBuilder = SchemaBuilder
      .builder(originalSchema.getNamespace)
      .record(originalSchema.getName)
      .fields()

    originalSchema.getFields.toList.foreach { f =>
      customSchemaBuilder = customSchemaBuilder
        .name(fieldNameMapping(f.name()))
        .`type`(f.schema())
        .noDefault()
    }

    customSchemaBuilder.endRecord()
  }

  def mapGenericRecordFromOriginalToTarget(record: GenericRecord, mappedSchema: Schema, fieldNameMapping: Map[String, String]): GenericRecord = {
    val customRecord = new org.apache.avro.generic.GenericData.Record(mappedSchema)
    record.getSchema.getFields.foreach{ f =>
      customRecord.put(fieldNameMapping(f.name()), record.get(f.name()))
    }
    customRecord
  }

  def mapGenericRecordFromTargetToOriginal(record: GenericRecord, schema: Schema, fieldNameMapping: Map[String, String]): GenericRecord = {
    val customRecord = new org.apache.avro.generic.GenericData.Record(schema)
    customRecord.getSchema.getFields.foreach{ f =>
      customRecord.put(f.name(), record.get(fieldNameMapping(f.name())))
    }
    customRecord
  }
}
