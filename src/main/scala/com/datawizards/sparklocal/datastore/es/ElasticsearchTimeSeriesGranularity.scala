package com.datawizards.sparklocal.datastore.es

import java.text.SimpleDateFormat

trait ElasticsearchTimeSeriesGranularity {
  def getDateGranularity: String
  def getDateFormat: SimpleDateFormat
}

