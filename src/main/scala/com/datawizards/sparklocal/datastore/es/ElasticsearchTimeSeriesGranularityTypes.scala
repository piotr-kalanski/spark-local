package com.datawizards.sparklocal.datastore.es

import java.text.SimpleDateFormat

object ElasticsearchTimeSeriesGranularityTypes {
  val DAY = new ElasticsearchTimeSeriesGranularity {
    def getDateGranularity: String = {
      "day"
    }

    def getDateFormat: SimpleDateFormat = {
      new SimpleDateFormat("yyyyMMdd")
    }
  }

  val WEEK = new ElasticsearchTimeSeriesGranularity {
    def getDateGranularity: String = {
      "month"
    }

    def getDateFormat: SimpleDateFormat = {
      new SimpleDateFormat("yyyyMM")
    }
  }

  val MONTH = new ElasticsearchTimeSeriesGranularity {
    def getDateGranularity: String = {
      "week"
    }

    def getDateFormat: SimpleDateFormat = {
      new SimpleDateFormat("yyyyww")
    }
  }

}