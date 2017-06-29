package org.elastic4play.models

import com.sksamuel.elastic4s.mappings.TypedFieldDefinition
import org.elastic4play.AttributeError
import org.elastic4play.controllers.{ JsonInputValue, NullInputValue, InputValue }
import play.api.libs.json.{ JsNull, JsValue }
import org.elastic4play.models.JsonFormat.optionFormat
import org.scalactic._

case class OptionalAttributeFormat[T](attributeFormat: AttributeFormat[T]) extends AttributeFormat[Option[T]]("optional-" + attributeFormat.name)(optionFormat(attributeFormat.jsFormat)) {
  override def checkJson(subNames: Seq[String], value: JsValue): Or[JsValue, Every[AttributeError]] = value match {
    case JsNull if subNames.isEmpty ⇒ Good(value)
    case _                          ⇒ attributeFormat.checkJson(subNames, value)
  }

  override def inputValueToJson(subNames: Seq[String], value: InputValue): JsValue Or Every[AttributeError] = value match {
    case NullInputValue | JsonInputValue(JsNull) ⇒ Good(JsNull)
    case x                                       ⇒ attributeFormat.inputValueToJson(subNames, x)
  }

  override def fromInputValue(subNames: Seq[String], value: InputValue): Option[T] Or Every[AttributeError] = value match {
    case NullInputValue ⇒ Good(None)
    case x              ⇒ attributeFormat.fromInputValue(subNames, x).map(v ⇒ Some(v))
  }

  override def elasticType(attributeName: String): TypedFieldDefinition = attributeFormat.elasticType(attributeName)
}
