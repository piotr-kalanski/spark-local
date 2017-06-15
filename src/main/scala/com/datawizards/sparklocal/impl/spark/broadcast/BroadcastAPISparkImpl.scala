package com.datawizards.sparklocal.impl.spark.broadcast

import com.datawizards.sparklocal.broadcast.BroadcastAPI
import org.apache.spark.broadcast.Broadcast

class BroadcastAPISparkImpl[T](broadcast: Broadcast[T]) extends BroadcastAPI[T] {
  override def value: T = broadcast.value

  override def unpersist(): Unit = broadcast.unpersist()

  override def unpersist(blocking: Boolean): Unit = broadcast.unpersist(blocking)

  override def destroy(): Unit = broadcast.destroy()
}
