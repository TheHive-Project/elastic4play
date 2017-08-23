package org.elastic4play.models

import java.util.Date

import scala.util.Try

import play.api.libs.json.{ JsNumber, JsString, JsValue }

import com.sksamuel.elastic4s.ElasticDsl.dateField
import com.sksamuel.elastic4s.mappings.BasicFieldDefinition
import org.scalactic._

import org.elastic4play.controllers.{ InputValue, JsonInputValue, StringInputValue }
import org.elastic4play.{ AttributeError, InvalidFormatAttributeError }

object DateAttributeFormat extends AttributeFormat[Date]("date") {
  def parse(d: String): Option[Date] = {
    Try {
      val datePattern = "yyyyMMdd'T'HHmmssZ"
      val df = new java.text.SimpleDateFormat(datePattern)
      df.setLenient(false)
      df.parse(d)
    }
      .orElse(Try(new Date(d.toLong)))
      .toOption
  }

  override def checkJson(subNames: Seq[String], value: JsValue): Or[JsValue, One[InvalidFormatAttributeError]] = value match {
    case JsString(v) if subNames.isEmpty ⇒ parse(v).map(_ ⇒ Good(value)).getOrElse(formatError(JsonInputValue(value)))
    case JsNumber(_) if subNames.isEmpty ⇒ Good(value)
    case _                               ⇒ formatError(JsonInputValue(value))
  }

  override def fromInputValue(subNames: Seq[String], value: InputValue): Date Or Every[AttributeError] = {
    if (subNames.nonEmpty)
      formatError(value)
    else {
      value match {
        case StringInputValue(Seq(v))    ⇒ parse(v).map(Good(_)).getOrElse(formatError(value))
        case JsonInputValue(JsString(v)) ⇒ parse(v).map(Good(_)).getOrElse(formatError(value))
        case JsonInputValue(JsNumber(v)) ⇒ Good(new Date(v.toLong))
        case _                           ⇒ formatError(value)
      }
    }
  }

  override def elasticType(attributeName: String): BasicFieldDefinition = dateField(attributeName).format("epoch_millis||basic_date_time_no_millis")
}