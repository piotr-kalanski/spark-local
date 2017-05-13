package com.datawizards.sparklocal.dataset.io

trait Reader {
  def read[T]: ReaderExecutor[T]
}
