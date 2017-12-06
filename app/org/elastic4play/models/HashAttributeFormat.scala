package org.elastic4play.models

import play.api.libs.json.{ JsString, JsValue }

import com.sksamuel.elastic4s.http.ElasticDsl.keywordField
import com.sksamuel.elastic4s.mappings.KeywordFieldDefinition
import org.scalactic._

import org.elastic4play.controllers.{ InputValue, JsonInputValue, StringInputValue }
import org.elastic4play.{ AttributeError, InvalidFormatAttributeError }

object HashAttributeFormat extends AttributeFormat[String]("hash") {
  val validDigits = "0123456789abcdefABCDEF"

  override def checkJson(subNames: Seq[String], value: JsValue): Or[JsValue, One[InvalidFormatAttributeError]] = value match {
    case JsString(v) if subNames.isEmpty && v.forall(c ⇒ validDigits.contains(c)) ⇒ Good(value)
    case _ ⇒ formatError(JsonInputValue(value))
  }

  override def fromInputValue(subNames: Seq[String], value: InputValue): String Or Every[AttributeError] = {
    if (subNames.nonEmpty)
      formatError(value)
    else
      value match {
        case StringInputValue(Seq(v)) if v.forall(c ⇒ validDigits.contains(c)) ⇒ Good(v.toLowerCase)
        case JsonInputValue(JsString(v)) if v.forall(c ⇒ validDigits.contains(c)) ⇒ Good(v.toLowerCase)
        case _ ⇒ formatError(value)
      }
  }

  override def elasticType(attributeName: String): KeywordFieldDefinition = keywordField(attributeName)
}