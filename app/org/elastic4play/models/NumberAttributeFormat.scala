package org.elastic4play.models

import play.api.libs.json.{ JsNumber, JsValue }

import com.sksamuel.elastic4s.ElasticDsl.longField
import com.sksamuel.elastic4s.mappings.BasicFieldDefinition
import org.scalactic._

import org.elastic4play.controllers.{ InputValue, JsonInputValue, StringInputValue }
import org.elastic4play.{ AttributeError, InvalidFormatAttributeError }

class NumberAttributeFormat extends AttributeFormat[Long]("number") {
  override def checkJson(subNames: Seq[String], value: JsValue): Or[JsValue, One[InvalidFormatAttributeError]] = value match {
    case _: JsNumber if subNames.isEmpty ⇒ Good(value)
    case _                               ⇒ formatError(JsonInputValue(value))
  }

  override def fromInputValue(subNames: Seq[String], value: InputValue): Long Or Every[AttributeError] = {
    if (subNames.nonEmpty)
      formatError(value)
    else
      value match {
        case StringInputValue(Seq(v)) ⇒ try {
          Good(v.toLong)
        }
        catch {
          case _: Throwable ⇒ formatError(value)
        }
        case JsonInputValue(JsNumber(v)) ⇒ Good(v.longValue)
        case _                           ⇒ formatError(value)
      }
  }

  override def elasticType(attributeName: String): BasicFieldDefinition = longField(attributeName)

}

object NumberAttributeFormat extends NumberAttributeFormat