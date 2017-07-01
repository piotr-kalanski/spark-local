package com.datawizards.sparklocal.impl.scala.broadcast

import com.datawizards.sparklocal.broadcast.BroadcastAPI

class BroadcastAPIScalaImpl[T](v: T) extends BroadcastAPI[T] {
  override def value: T = v

  override def unpersist(): Unit = { /* do nothing */ }

  override def unpersist(blocking: Boolean): Unit = { /* do nothing */ }

  override def destroy(): Unit = { /* do nothing */ }
}
