package com.datawizards.sparklocal.dataset.io

import com.datawizards.csv2class._
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.datastore._
import com.sksamuel.avro4s.{FromRecord, SchemaFor, ToRecord}
import org.apache.spark.sql.Encoder
import shapeless.{Generic, HList}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

trait ReaderExecutor[T] {
  def apply[L <: HList](dataStore: CSVDataStore)
                       (implicit
                          ct: ClassTag[T],
                          tt: TypeTag[T],
                          gen: Generic.Aux[T, L],
                          fromRow: FromRow[L],
                          enc: Encoder[T]
                       ): DataSetAPI[T]

  def apply(dataStore: JsonDataStore)
           (implicit
              ct: ClassTag[T],
              tt: TypeTag[T]
           ): DataSetAPI[T]

  def apply(dataStore: ParquetDataStore)
           (implicit
              ct: ClassTag[T],
              tt: TypeTag[T],
              s: SchemaFor[T],
              fromR: FromRecord[T],
              toR: ToRecord[T],
              enc: Encoder[T]
           ): DataSetAPI[T]

  def apply(dataStore: AvroDataStore)
           (implicit
              ct: ClassTag[T],
              tt: TypeTag[T],
              s: SchemaFor[T],
              r: FromRecord[T],
              enc: Encoder[T]
           ): DataSetAPI[T]

  def apply(dataStore: HiveDataStore)
           (implicit
              ct: ClassTag[T],
              tt: TypeTag[T],
              s: SchemaFor[T],
              r: FromRecord[T],
              enc: Encoder[T]
           ): DataSetAPI[T]

  def apply[L <: HList](dataStore: DataStore)
           (implicit
            ct: ClassTag[T],
            tt: TypeTag[T],
            gen: Generic.Aux[T, L],
            fromRow: FromRow[L],
            s: SchemaFor[T],
            fromR: FromRecord[T],
            toR: ToRecord[T],
            encoder: Encoder[T]
           ): DataSetAPI[T] = dataStore match {
    case d:CSVDataStore => this.apply(d)
    case d:JsonDataStore => this.apply(d)
    case d:ParquetDataStore => this.apply(d)
    case d:AvroDataStore => this.apply(d)
    case d:JdbcDataStore => this.apply(d)
    case d:HiveDataStore => this.apply(d)
    case _ => throw new IllegalArgumentException("Not supported: " + dataStore)
  }

  def apply[L <: HList](dataStore: JdbcDataStore)
                       (implicit
                          ct: ClassTag[T],
                          tt: TypeTag[T],
                          gen: Generic.Aux[T, L],
                          fromRow: FromRow[L],
                          enc: Encoder[T]
                       ): DataSetAPI[T]

  def csv[L <: HList](dataStore: CSVDataStore)
                       (implicit
                        ct: ClassTag[T],
                        tt: TypeTag[T],
                        gen: Generic.Aux[T, L],
                        fromRow: FromRow[L],
                        enc: Encoder[T]
                       ): DataSetAPI[T] =
    this.apply(dataStore)

  def json(dataStore: JsonDataStore)
           (implicit
            ct: ClassTag[T],
            tt: TypeTag[T]
           ): DataSetAPI[T] =
    this.apply(dataStore)

  def parquet(dataStore: ParquetDataStore)
           (implicit
            ct: ClassTag[T],
            tt: TypeTag[T],
            s: SchemaFor[T],
            fromR: FromRecord[T],
            toR: ToRecord[T],
            enc: Encoder[T]
           ): DataSetAPI[T] =
    this.apply(dataStore)

  def avro(dataStore: AvroDataStore)
           (implicit
            ct: ClassTag[T],
            tt: TypeTag[T],
            s: SchemaFor[T],
            r: FromRecord[T],
            enc: Encoder[T]
           ): DataSetAPI[T] =
    this.apply(dataStore)

  def table(dataStore: HiveDataStore)
           (implicit
            ct: ClassTag[T],
            tt: TypeTag[T],
            s: SchemaFor[T],
            r: FromRecord[T],
            enc: Encoder[T]
           ): DataSetAPI[T] =
    this.apply(dataStore)

  def jdbc[L <: HList](dataStore: JdbcDataStore)
                       (implicit
                        ct: ClassTag[T],
                        tt: TypeTag[T],
                        gen: Generic.Aux[T, L],
                        fromRow: FromRow[L],
                        enc: Encoder[T]
                       ): DataSetAPI[T] =
    this.apply(dataStore)
}
