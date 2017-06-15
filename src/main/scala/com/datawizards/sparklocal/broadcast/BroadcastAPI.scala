package com.datawizards.sparklocal.broadcast

trait BroadcastAPI[T] extends Serializable {
  /** Get the broadcasted value. */
  def value: T
  /**
    * Asynchronously delete cached copies of this broadcast on the executors.
    * If the broadcast is used after this is called, it will need to be re-sent to each executor.
    */
  def unpersist()
  /**
    * Delete cached copies of this broadcast on the executors. If the broadcast is used after
    * this is called, it will need to be re-sent to each executor.
    * @param blocking Whether to block until unpersisting has completed
    */
  def unpersist(blocking: Boolean)
  /**
    * Destroy all data and metadata related to this broadcast variable. Use this with caution;
    * once a broadcast variable has been destroyed, it cannot be used again.
    * This method blocks until destroy has completed
    */
  def destroy()
}
