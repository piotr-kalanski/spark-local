package com.datawizards.sparklocal.dataset.expressions

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit}

import scala.language.reflectiveCalls
import scala.language.implicitConversions

object Expressions {

  trait Expression {
    type ResultType
    def eval(instance: Any, that: Any): ResultType
    def toSparkColumn: Column
  }

  trait BooleanExpression extends Expression {
    override type ResultType = Boolean

    def &&(other: BooleanExpression): And = new And(this,other)

    def ||(other: BooleanExpression): Or = new Or(this,other)

    def unary_! : BooleanExpression = new Not(this)
  }

  trait Value extends Expression {
    override type ResultType = Any

    def ===(other: Value): BooleanExpression = new Eq(this, other)
    def =!=(other: Value): BooleanExpression = new Not(new Eq(this, other))
//    def > (other: Any): BooleanExpression = this > new Literal(other)
//    def > (other: Value): BooleanExpression = new Gt(this, other)
  }

  class Field(name: String) extends Value {
    def eval(instance: Any, that: Any): Any = {
      val instanceClass = instance.getClass
      val thatClass = that.getClass

      val (selectedInstance, field) =
        if(instanceClass.getDeclaredFields.exists(_.getName == name))
          (instance, instanceClass.getDeclaredField(name))
        else
          (that, thatClass.getDeclaredField(name))

      field.setAccessible(true)
      field.get(selectedInstance)
    }

    override def toSparkColumn: Column =
      col(name)
  }

  class Literal(value: Any) extends Value {
    override def eval(instance: Any, that: Any): Any =
      value

    override def toSparkColumn: Column =
      lit(value)
  }

  class Eq(left: Value, right: Value) extends BooleanExpression {
    override def eval(instance: Any, that: Any): Boolean =
      left.eval(instance, that) == right.eval(instance, that)

    override def toSparkColumn: Column =
      left.toSparkColumn === right.toSparkColumn
  }

//  class Gt(left: Value, right: Value) extends BooleanExpression {
//    override def eval(instance: Any, that: Any): Boolean = ???
//
//    override def toSparkColumn: Column =
//      left.toSparkColumn > right.toSparkColumn
//  }

  class Not(expression: BooleanExpression) extends BooleanExpression {
    override def eval(instance: Any, that: Any): Boolean =
      !expression.eval(instance, that)

    override def toSparkColumn: Column =
      !expression.toSparkColumn
  }

  class And(left: BooleanExpression, right: BooleanExpression) extends BooleanExpression {
    override def eval(instance: Any, that: Any): Boolean =
      left.eval(instance, that) && right.eval(instance, that)

    override def toSparkColumn: Column =
      left.toSparkColumn && right.toSparkColumn
  }

  class Or(left: BooleanExpression, right: BooleanExpression) extends BooleanExpression {
    override def eval(instance: Any, that: Any): Boolean =
      left.eval(instance, that) || right.eval(instance, that)

    override def toSparkColumn: Column =
      left.toSparkColumn || right.toSparkColumn
  }

}
