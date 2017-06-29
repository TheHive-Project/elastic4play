package org.elastic4play.models

import java.util.Date

import com.sksamuel.elastic4s.ElasticDsl.field
import com.sksamuel.elastic4s.mappings.DateFieldDefinition
import com.sksamuel.elastic4s.mappings.FieldType.DateType
import org.elastic4play.controllers.{ InputValue, JsonInputValue, StringInputValue }
import org.elastic4play.{ AttributeError, InvalidFormatAttributeError }
import org.scalactic._
import play.api.libs.json.{ JsNumber, JsString, JsValue }

import scala.util.Try

object DateAttributeFormat extends AttributeFormat[Date]("date") {
  def parse(d: String): Option[Date] = {
    Try {
      val datePattern = "yyyyMMdd'T'HHmmssZ" // FIXME
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

  override def elasticType(attributeName: String): DateFieldDefinition = field(attributeName, DateType) format "epoch_millis||basic_date_time_no_millis"
}