package org.elastic4play.models

import com.sksamuel.elastic4s.ElasticDsl.field
import com.sksamuel.elastic4s.mappings.BinaryFieldDefinition
import com.sksamuel.elastic4s.mappings.FieldType.BinaryType
import org.elastic4play.controllers.{ JsonInputValue, InputValue }
import org.elastic4play.{ AttributeError, InvalidFormatAttributeError }
import play.api.libs.json.JsValue
import org.elastic4play.models.JsonFormat.binaryFormats
import org.scalactic._

object BinaryAttributeFormat extends AttributeFormat[Array[Byte]]("binary")(binaryFormats) {
  override def checkJson(subNames: Seq[String], value: JsValue): Bad[JsValue, One[InvalidFormatAttributeError]] = formatError(JsonInputValue(value))

  override def fromInputValue(subNames: Seq[String], value: InputValue): Array[Byte] Or Every[AttributeError] = formatError(value)

  override def elasticType(attributeName: String): BinaryFieldDefinition = field(attributeName, BinaryType)
}
