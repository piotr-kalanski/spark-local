package com.datawizards.sparklocal.dataset.expr

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

object Expressions {

  trait Expression {
    abstract type T
    def eval(): T
    def toSparkColumn: Column
  }

  trait BooleanExpression extends Expression {
    override abstract type T = Boolean
  }

  class Field[T](name: String) extends Expression {

    override def eval(): Field.this.type = ??? //TODO

    override def toSparkColumn: Column = col(name)
  }

  class Eq[T](left: Field[T], right: Field[T]) extends BooleanExpression {
    override def eval(): Boolean = left.eval() == right.eval()

    override def toSparkColumn: Column = left.toSparkColumn === right.toSparkColumn
  }

  class Not(expression: BooleanExpression) extends BooleanExpression {
    override def eval(): Boolean = !expression.eval()

    override def toSparkColumn: Column = !expression.toSparkColumn
  }

  class And(expressions: BooleanExpression*) extends BooleanExpression {
    override def eval(): Boolean = expressions.forall(_.eval())

    override def toSparkColumn: Column = {
      var result = expressions.head.toSparkColumn

      for(e <- expressions.tail)
        result = result && e.toSparkColumn

      result
    }
  }

  class Or(expressions: BooleanExpression*) extends BooleanExpression {
    override def eval(): Boolean = expressions.exists(_.eval())

    override def toSparkColumn: Column = {
      var result = expressions.head.toSparkColumn

      for(e <- expressions.tail)
        result = result || e.toSparkColumn

      result
    }

  }

}
