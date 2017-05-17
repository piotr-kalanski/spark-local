package com.datawizards.sparklocal.dataset.io

import com.datawizards.csv2class.FromRow
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.datastore._
import com.sksamuel.avro4s.{FromRecord, SchemaFor, ToRecord}
import shapeless.{Generic, HList}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

trait ReaderExecutor[T] {
  def apply[L <: HList](dataStore: CSVDataStore)(
    implicit
    ct: ClassTag[T],
    tt: TypeTag[T],
    gen: Generic.Aux[T, L],
    fromRow: FromRow[L]
  ): DataSetAPI[T]

  def apply(dataStore: JsonDataStore)(
    implicit
    ct: ClassTag[T],
    tt: TypeTag[T]
  ): DataSetAPI[T]

  def apply(dataStore: ParquetDataStore)(
    implicit
    ct: ClassTag[T],
    tt: TypeTag[T],
    s: SchemaFor[T],
    fromR: FromRecord[T],
    toR: ToRecord[T]
  ): DataSetAPI[T]

  def apply(dataStore: AvroDataStore)(
    implicit
    ct: ClassTag[T],
    tt: TypeTag[T],
    s: SchemaFor[T],
    r: FromRecord[T]
  ): DataSetAPI[T]

  def apply(dataStore: HiveDataStore) (
    implicit
    ct: ClassTag[T],
    tt: TypeTag[T],
    s: SchemaFor[T],
    r: FromRecord[T]
  ): DataSetAPI[T]

}
