package org.elastic4play.models

import play.api.Logger
import play.api.libs.json.{ Format, JsArray, JsNull, JsValue }

import com.sksamuel.elastic4s.mappings.{ BasicFieldDefinition, FieldDefinition }
import org.scalactic._

import org.elastic4play.controllers.InputValue
import org.elastic4play.services.DBLists
import org.elastic4play.{ AttributeError, InvalidFormatAttributeError, MissingAttributeError, UpdateReadOnlyAttributeError }

case class AttributeDefinition(name: String, `type`: String, description: String, values: Seq[JsValue], labels: Seq[String])

abstract class AttributeFormat[T](val name: String)(implicit val jsFormat: Format[T]) {
  def checkJson(subNames: Seq[String], value: JsValue): JsValue Or Every[AttributeError]

  def checkJsonForCreation(subNames: Seq[String], value: JsValue): JsValue Or Every[AttributeError] =
    checkJson(subNames, value)

  def checkJsonForUpdate(subNames: Seq[String], value: JsValue): JsValue Or Every[AttributeError] =
    checkJson(subNames, value)

  def inputValueToJson(subNames: Seq[String], value: InputValue): JsValue Or Every[AttributeError] = fromInputValue(subNames, value).map(v ⇒ jsFormat.writes(v))

  def fromInputValue(subNames: Seq[String], value: InputValue): T Or Every[AttributeError]

  def elasticType(attributeName: String): FieldDefinition

  protected def formatError(value: InputValue) = Bad(One(InvalidFormatAttributeError("", name, value)))

  def definition(dblists: DBLists, attribute: Attribute[T]): Seq[AttributeDefinition] =
    Seq(AttributeDefinition(
      attribute.name,
      name,
      attribute.description,
      Nil,
      Nil))
}

object AttributeFormat {
  val dateFmt = DateAttributeFormat
  val textFmt = TextAttributeFormat
  val stringFmt = StringAttributeFormat
  val userFmt = UserAttributeFormat
  val booleanFmt = BooleanAttributeFormat
  val numberFmt = NumberAttributeFormat
  val attachmentFmt = AttachmentAttributeFormat
  val metricsFmt = MetricsAttributeFormat
  val customFields = CustomAttributeFormat
  val uuidFmt = UUIDAttributeFormat
  val hashFmt = HashAttributeFormat
  val binaryFmt = BinaryAttributeFormat

  def enumFmt[T <: Enumeration](e: T)(implicit format: Format[T#Value]): EnumerationAttributeFormat[T] = EnumerationAttributeFormat[T](e)

  def listEnumFmt(enumerationName: String)(dblists: DBLists): ListEnumerationAttributeFormat = ListEnumerationAttributeFormat(enumerationName)(dblists)

  def objectFmt(subAttributes: Seq[Attribute[_]]) = ObjectAttributeFormat(subAttributes)
}

object AttributeOption extends Enumeration with HiveEnumeration {
  type Type = Value
  val readonly, unaudited, model, form, sensitive = Value
}

case class Attribute[T](
    modelName: String,
    name: String,
    format: AttributeFormat[T],
    options: Seq[AttributeOption.Type],
    defaultValue: Option[() ⇒ T],
    description: String) {
  private[Attribute] lazy val logger = Logger(getClass)

  def defaultValueJson: Option[JsValue] = defaultValue.map(d ⇒ format.jsFormat.writes(d()))

  lazy val isMulti: Boolean = format match {
    case _: MultiAttributeFormat[_] ⇒ true
    case _                          ⇒ false
  }
  lazy val isForm: Boolean = !options.contains(AttributeOption.model)
  lazy val isModel: Boolean = !options.contains(AttributeOption.form)
  lazy val isReadonly: Boolean = options.contains(AttributeOption.readonly)
  lazy val isUnaudited: Boolean = options.contains(AttributeOption.unaudited) || isSensitive || isReadonly
  lazy val isSensitive: Boolean = options.contains(AttributeOption.sensitive)
  lazy val isRequired: Boolean = format match {
    case _: OptionalAttributeFormat[_] ⇒ false
    case _: MultiAttributeFormat[_]    ⇒ false
    case _                             ⇒ true
  }

  def elasticMapping: FieldDefinition = format.elasticType(name) match {
    case a: BasicFieldDefinition if isSensitive ⇒ a.index("no")
    case a                                      ⇒ a
  }

  def validateForCreation(value: Option[JsValue]): Option[JsValue] Or Every[AttributeError] = {
    val result = value match {
      case Some(JsNull) if !isRequired         ⇒ Good(value)
      case Some(JsArray(Seq())) if !isRequired ⇒ Good(value)
      case None if !isRequired                 ⇒ Good(value)
      case Some(JsNull) | Some(JsArray(Seq())) | None ⇒
        if (defaultValueJson.isDefined)
          Good(defaultValueJson)
        else
          Bad(One(MissingAttributeError(name)))
      case Some(v) ⇒
        format.checkJsonForCreation(Nil, v).transform(g ⇒ Good(Some(g)), x ⇒ Bad(x.map {
          case ifae: InvalidFormatAttributeError ⇒ ifae.copy(name = name)
          case other                             ⇒ other
        }))
    }
    logger.debug(s"$modelName.$name(${format.name}).validateForCreation($value) => $result")
    result
  }

  def validateForUpdate(subNames: Seq[String], value: JsValue): JsValue Or Every[AttributeError] = {
    val result = value match {
      case _ if isReadonly                       ⇒ Bad(One(UpdateReadOnlyAttributeError(name)))
      case JsNull | JsArray(Seq()) if isRequired ⇒ Bad(One(MissingAttributeError(name)))
      case JsNull | JsArray(Seq())               ⇒ Good(value)
      case v ⇒
        format.checkJsonForUpdate(subNames, v).badMap(_.map {
          case ifae: InvalidFormatAttributeError ⇒ ifae.copy(name = name)
          case other                             ⇒ other
        })
    }
    logger.debug(s"$modelName.$name(${format.name}).validateForUpdate($value) => $result")
    result
  }
}
