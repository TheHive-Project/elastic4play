package org.elastic4play.utils

import java.util.{ Date ⇒ JDate }

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

object Date {
  val datePattern = "yyyyMMdd'T'HHmmssZ"
  implicit class RichDate(date: JDate) {
    def toIso: String = new java.text.SimpleDateFormat(datePattern).format(date)
  }

  implicit class RichJoda(date: DateTime) {
    def toIso: String = date.toString(DateTimeFormat.forPattern(datePattern))
  }
}