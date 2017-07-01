package com.datawizards.sparklocal.impl.scala.dataset.io

import com.datawizards.dmg.dialects.Dialect
import com.datawizards.dmg.metadata.MetaDataExtractor
import com.datawizards.sparklocal.dataset.io.JSONDialect
import org.json4s.JValue
import org.json4s.JsonAST.JObject

import scala.reflect.runtime.universe.TypeTag


object FieldNamesMappingUtils {
  def constructFieldNameMapping[T: TypeTag](dialect: Dialect, fromOriginal: Boolean=true): Map[String, String] = {
    val classTypeMetaData = MetaDataExtractor.extractClassMetaDataForDialect[T](dialect)
    classTypeMetaData
      .fields
      .map{ f =>
        if(fromOriginal) f.originalFieldName -> f.fieldName
        else f.fieldName -> f.originalFieldName
      }
      .toMap
  }

  def changeJsonObjectFieldNames[T: TypeTag](value: JValue, fromOriginal: Boolean): JValue = value match {
    case JObject(fields) =>
      val fieldNameMapping = FieldNamesMappingUtils.constructFieldNameMapping[T](JSONDialect, fromOriginal)
      JObject(
        fields.map{case (fieldName, fieldValue) =>
          (fieldNameMapping.getOrElse(fieldName, fieldName), fieldValue)
        }
      )
    case other => other
  }
}
