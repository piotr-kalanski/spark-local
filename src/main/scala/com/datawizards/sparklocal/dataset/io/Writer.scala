package com.datawizards.sparklocal.dataset.io

import com.datawizards.sparklocal.dataset.DataSetAPI

trait Writer[T] {
  def write(ds: DataSetAPI[T]): WriterExecutor[T]
}
