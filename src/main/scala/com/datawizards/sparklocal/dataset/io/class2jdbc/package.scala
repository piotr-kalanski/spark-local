package com.datawizards.sparklocal.dataset.io

import java.util.Date

import shapeless._

import scala.reflect.ClassTag

package object class2jdbc {

  trait JdbcEncoder[T] {
    def encode(value: T): List[String]
  }

  def createEncoder[A](func: A => List[String]): JdbcEncoder[A] =
    new JdbcEncoder[A] {
      def encode(value: A): List[String] =
        func(value)
    }

  implicit val stringEnc: JdbcEncoder[String] =
    createEncoder(str => List("'" + str + "'"))

  implicit val intEnc: JdbcEncoder[Int] =
    createEncoder(num => List(num.toString))

  implicit val booleanEnc: JdbcEncoder[Boolean] =
    createEncoder(bool => List(bool.toString))

  implicit val longEnc: JdbcEncoder[Long] =
    createEncoder(num => List(num.toString))

  implicit val doubleEnc: JdbcEncoder[Double] =
    createEncoder(num => List(num.toString))

  implicit val floatEnc: JdbcEncoder[Float] =
    createEncoder(num => List(num.toString))

  implicit val shortEnc: JdbcEncoder[Short] =
    createEncoder(num => List(num.toString))

  implicit val charEnc: JdbcEncoder[Char] =
    createEncoder(num => List("'" + num.toString + "'"))

  implicit val byteEnc: JdbcEncoder[Byte] =
    createEncoder(num => List(num.toString))

  implicit val dateEnc: JdbcEncoder[Date] =
    createEncoder(date => List("'" + date.toString + "'"))

  implicit val bigIntEnc: JdbcEncoder[BigInt] =
    createEncoder(num => List(num.toString))

  implicit val hnilEncoder: JdbcEncoder[HNil] =
    createEncoder(hnil => Nil)

  implicit def hlistEncoder[H, T <: HList](
    implicit
    hEncoder: Lazy[JdbcEncoder[H]],
    tEncoder: JdbcEncoder[T]
  ): JdbcEncoder[H :: T] = createEncoder {
    case h :: t =>
      hEncoder.value.encode(h) ++ tEncoder.encode(t)
  }

  implicit def genericEncoder[A, R](
                                     implicit
                                     gen: Generic.Aux[A, R],
                                     rEncoder: Lazy[JdbcEncoder[R]]
                                   ): JdbcEncoder[A] = createEncoder { value =>
    rEncoder.value.encode(gen.to(value))
  }

  def generateInserts[T](data: Traversable[T], table: String, columns: Seq[String])
                        (implicit encoder: JdbcEncoder[T]): Traversable[String] = {
    val insertPrefix = "INSERT INTO " + table + "(" + columns.mkString(",") + ")"  + " VALUES("

    data
      .map(e => insertPrefix + encoder.encode(e).mkString(",") + ")")
  }

  def generateInserts[T](data: Traversable[T], table: String)
                        (implicit ct:ClassTag[T], encoder: JdbcEncoder[T]): Traversable[String] = {
    val klass = ct.runtimeClass
    generateInserts(data, table, klass.getDeclaredFields.map(_.getName))
  }

  def generateInserts[T](data: Traversable[T])
                       (implicit ct:ClassTag[T], encoder: JdbcEncoder[T]): Traversable[String] = {
    val klass = ct.runtimeClass
    val table = klass.getSimpleName
    val columns = klass.getDeclaredFields.map(_.getName)
    generateInserts(data, table, columns)
  }

}
