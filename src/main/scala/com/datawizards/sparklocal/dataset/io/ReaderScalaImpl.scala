package com.datawizards.sparklocal.dataset.io

import com.datawizards.csv2class
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.datastore

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import com.datawizards.csv2class._
import shapeless.Generic.Aux
import shapeless.HList

object ReaderScalaImpl extends Reader {

  override def read[T]: ReaderExecutor[T] = new ReaderExecutor[T] {
    override def apply[L <: HList](dataStore: datastore.CSVDataStore)(implicit ct: ClassTag[T], tt: TypeTag[T], gen: Aux[T, L], fromRow: csv2class.FromRow[L]): DataSetAPI[T] = {
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

    override def apply[L <: HList](dataStore: datastore.JsonDataStore)(implicit ct: ClassTag[T], tt: TypeTag[T], gen: Aux[T, L]): DataSetAPI[T] =
      ???

    override def apply[L <: HList](dataStore: datastore.ParquetDataStore)(implicit ct: ClassTag[T], tt: TypeTag[T], gen: Aux[T, L]): DataSetAPI[T] =
      ???

    override def apply[L <: HList](dataStore: datastore.AvroDataStore)(implicit ct: ClassTag[T], tt: TypeTag[T], gen: Aux[T, L]): DataSetAPI[T] =
      ???
  }

}
