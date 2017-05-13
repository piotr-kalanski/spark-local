package com.datawizards.sparklocal.dataset.io

import com.datawizards.csv2class.FromRow
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.datastore._
import shapeless.{Generic, HList}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

trait Reader {

  trait ReaderExecutor[T] {
    def apply[L <: HList](dataStore: CSVDataStore[T])(
      implicit
        ct: ClassTag[T],
        tt: TypeTag[T],
        gen: Generic.Aux[T, L],
        fromRow: FromRow[L]
    ): DataSetAPI[T]

    def apply[L <: HList](dataStore: JsonDataStore[T])(
      implicit
      ct: ClassTag[T],
      tt: TypeTag[T],
      gen: Generic.Aux[T, L]
    ): DataSetAPI[T]

    def apply[L <: HList](dataStore: ParquetDataStore[T])(
      implicit
      ct: ClassTag[T],
      tt: TypeTag[T],
      gen: Generic.Aux[T, L]
    ): DataSetAPI[T]

    def apply[L <: HList](dataStore: AvroDataStore[T])(
      implicit
      ct: ClassTag[T],
      tt: TypeTag[T],
      gen: Generic.Aux[T, L]
    ): DataSetAPI[T]
  }

  def read[T]: ReaderExecutor[T]
}
