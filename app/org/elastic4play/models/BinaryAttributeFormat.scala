package org.elastic4play.models

import play.api.libs.json.JsValue

import com.sksamuel.elastic4s.ElasticDsl.binaryField
import com.sksamuel.elastic4s.mappings.BasicFieldDefinition
import org.scalactic._

import org.elastic4play.controllers.{ InputValue, JsonInputValue }
import org.elastic4play.models.JsonFormat.binaryFormats
import org.elastic4play.{ AttributeError, InvalidFormatAttributeError }

object BinaryAttributeFormat extends AttributeFormat[Array[Byte]]("binary")(binaryFormats) {
  override def checkJson(subNames: Seq[String], value: JsValue): Bad[One[InvalidFormatAttributeError]] = formatError(JsonInputValue(value))

  override def fromInputValue(subNames: Seq[String], value: InputValue): Array[Byte] Or Every[AttributeError] = formatError(value)

  override def elasticType(attributeName: String): BasicFieldDefinition = binaryField(attributeName)
}
