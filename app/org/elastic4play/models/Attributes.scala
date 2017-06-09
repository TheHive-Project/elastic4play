package org.elastic4play.models

import java.util
import java.util.{ Date, UUID }

import scala.language.{ existentials, implicitConversions, postfixOps }
import scala.math.BigDecimal.{ int2bigDecimal, long2bigDecimal }
import scala.reflect.ClassTag
import scala.util.Try
import play.api.Logger
import play.api.libs.json._
import play.api.libs.json.Json.toJsFieldJsValueWrapper
import org.elastic4play._
import org.elastic4play.JsonFormat.dateFormat
import org.elastic4play.controllers._
import org.elastic4play.controllers.JsonFormat.{ fileInputValueFormat, inputValueFormat }
import org.elastic4play.models.JsonFormat.{ binaryFormats, multiFormat, optionFormat }
import org.elastic4play.services.{ Attachment, DBLists }
import org.elastic4play.services.JsonFormat.attachmentFormat
import org.scalactic._
import org.scalactic.Accumulation.convertGenTraversableOnceToValidatable
import com.sksamuel.elastic4s.ElasticDsl.field
import com.sksamuel.elastic4s.mappings.FieldType._
import com.sksamuel.elastic4s.mappings._

abstract class AttributeFormat[T](val name: String)(implicit val jsFormat: Format[T]) {
  def checkJson(subNames: Seq[String], value: JsValue): JsValue Or Every[AttributeError]
  def checkJsonForCreation(subNames: Seq[String], value: JsValue): JsValue Or Every[AttributeError] =
    checkJson(subNames, value)
  def checkJsonForUpdate(subNames: Seq[String], value: JsValue): JsValue Or Every[AttributeError] =
    checkJson(subNames, value)
  def inputValueToJson(subNames: Seq[String], value: InputValue): JsValue Or Every[AttributeError] = fromInputValue(subNames, value).map(v ⇒ jsFormat.writes(v))
  def elasticToJson(values: Seq[Any]): Option[JsValue]
  def fromInputValue(subNames: Seq[String], value: InputValue): T Or Every[AttributeError]
  def swaggerType: JsObject
  def elasticType(attributeName: String): TypedFieldDefinition
}

object TextAttributeFormat extends AttributeFormat[String]("text") {
  override def checkJson(subNames: Seq[String], value: JsValue): Or[JsValue, One[InvalidFormatAttributeError]] = value match {
    case _: JsString if subNames.isEmpty ⇒ Good(value)
    case _                               ⇒ Bad(One(InvalidFormatAttributeError("", name, JsonInputValue(value))))
  }

  override def fromInputValue(subNames: Seq[String], value: InputValue): String Or Every[AttributeError] = {
    if (subNames.nonEmpty)
      Bad(One(InvalidFormatAttributeError("", name, value)))
    else
      value match {
        case StringInputValue(Seq(v))    ⇒ Good(v)
        case JsonInputValue(JsString(v)) ⇒ Good(v)
        case _                           ⇒ Bad(One(InvalidFormatAttributeError("", name, value)))
      }
  }

  override def elasticToJson(values: Seq[Any]): Option[JsValue] = values match {
    case Seq(s: String) ⇒ Some(JsString(s))
    case _              ⇒ None
  }

  override val swaggerType: JsObject = Json.obj("type" → "string")

  override def elasticType(attributeName: String): StringFieldDefinition = field(attributeName, StringType)
}

object StringAttributeFormat extends AttributeFormat[String]("string") {
  override def checkJson(subNames: Seq[String], value: JsValue): Or[JsValue, One[InvalidFormatAttributeError]] = value match {
    case _: JsString if subNames.isEmpty ⇒ Good(value)
    case _                               ⇒ Bad(One(InvalidFormatAttributeError("", name, JsonInputValue(value))))
  }

  override def fromInputValue(subNames: Seq[String], value: InputValue): String Or Every[AttributeError] = TextAttributeFormat.fromInputValue(subNames, value) match {
    case Bad(One(ifae: InvalidFormatAttributeError)) ⇒ Bad(One(ifae.copy(format = name)))
    case other                                       ⇒ other
  }

  override def elasticToJson(values: Seq[Any]): Option[JsValue] = values match {
    case Seq(s: String) ⇒ Some(JsString(s))
    case _              ⇒ None
  }

  override val swaggerType: JsObject = Json.obj("type" → "string")

  override def elasticType(attributeName: String): StringFieldDefinition = field(attributeName, StringType) index "not_analyzed"
}

object DateAttributeFormat extends AttributeFormat[Date]("date") {
  def parse(d: String): Date = {
    Try {
      val datePattern = "yyyyMMdd'T'HHmmssZ" // FIXME
      val df = new java.text.SimpleDateFormat(datePattern)
      df.setLenient(false)
      df.parse(d)
    } getOrElse {
      new Date(d.toLong)
    }
  }

  override def checkJson(subNames: Seq[String], value: JsValue): Or[JsValue, One[InvalidFormatAttributeError]] = value match {
    case JsString(v) if subNames.isEmpty ⇒ try { parse(v); Good(value) } catch { case _: Throwable ⇒ Bad(One(InvalidFormatAttributeError("", name, JsonInputValue(value)))) }
    case JsNumber(_) if subNames.isEmpty ⇒ Good(value)
    case _                               ⇒ Bad(One(InvalidFormatAttributeError("", name, JsonInputValue(value))))
  }

  override def fromInputValue(subNames: Seq[String], value: InputValue): Date Or Every[AttributeError] = {
    if (subNames.nonEmpty)
      Bad(One(InvalidFormatAttributeError("", name, value)))
    else {
      value match {
        case StringInputValue(Seq(v))    ⇒ try { Good(parse(v)) } catch { case _: Throwable ⇒ Bad(One(InvalidFormatAttributeError("", name, value))) }
        case JsonInputValue(JsString(v)) ⇒ try { Good(parse(v)) } catch { case _: Throwable ⇒ Bad(One(InvalidFormatAttributeError("", name, value))) }
        case JsonInputValue(JsNumber(v)) ⇒ try { Good(new Date(v.toLong)) } catch { case _: Throwable ⇒ Bad(One(InvalidFormatAttributeError("", name, value))) }
        case _                           ⇒ Bad(One(InvalidFormatAttributeError("", name, value)))
      }
    }
  }

  override def elasticToJson(values: Seq[Any]): Option[JsValue] = values match { // FIXME
    case Seq(s: String) ⇒ Some(JsString(s))
    case Seq(n: Number) ⇒ Some(JsNumber(n.longValue))
    case _              ⇒ None
  }

  override val swaggerType: JsObject = Json.obj("type" → "dateTime")

  override def elasticType(attributeName: String): DateFieldDefinition = field(attributeName, DateType) format "epoch_millis||basic_date_time_no_millis"
}

object BooleanAttributeFormat extends AttributeFormat[Boolean]("boolean") {
  override def checkJson(subNames: Seq[String], value: JsValue): Or[JsValue, One[InvalidFormatAttributeError]] = value match {
    case _: JsBoolean if subNames.isEmpty ⇒ Good(value)
    case _                                ⇒ Bad(One(InvalidFormatAttributeError("", name, JsonInputValue(value))))
  }

  override def fromInputValue(subNames: Seq[String], value: InputValue): Boolean Or Every[AttributeError] = {
    if (subNames.nonEmpty)
      Bad(One(InvalidFormatAttributeError("", name, value)))
    else
      value match {
        case StringInputValue(Seq(v))     ⇒ try { Good(v.toBoolean) } catch { case _: Throwable ⇒ Bad(One(InvalidFormatAttributeError("", name, value))) }
        case JsonInputValue(JsBoolean(v)) ⇒ Good(v)
        case _                            ⇒ Bad(One(InvalidFormatAttributeError("", name, value)))
      }
  }

  override def elasticToJson(values: Seq[Any]): Option[JsValue] = values match {
    case Seq(b: Boolean) ⇒ Some(JsBoolean(b))
    case _               ⇒ None
  }

  override val swaggerType: JsObject = Json.obj("type" → "boolean")

  override def elasticType(attributeName: String): BooleanFieldDefinition = field(attributeName, BooleanType)
}

object NumberAttributeFormat extends AttributeFormat[Long]("number") {
  override def checkJson(subNames: Seq[String], value: JsValue): Or[JsValue, One[InvalidFormatAttributeError]] = value match {
    case _: JsNumber if subNames.isEmpty ⇒ Good(value)
    case _                               ⇒ Bad(One(InvalidFormatAttributeError("", name, JsonInputValue(value))))
  }

  override def fromInputValue(subNames: Seq[String], value: InputValue): Long Or Every[AttributeError] = {
    if (subNames.nonEmpty)
      Bad(One(InvalidFormatAttributeError("", name, value)))
    else
      value match {
        case StringInputValue(Seq(v))    ⇒ try { Good(v.toLong) } catch { case _: Throwable ⇒ Bad(One(InvalidFormatAttributeError("", name, value))) }
        case JsonInputValue(JsNumber(v)) ⇒ Good(v.longValue)
        case _                           ⇒ Bad(One(InvalidFormatAttributeError("", name, value)))
      }
  }

  override def elasticToJson(values: Seq[Any]): Option[JsValue] = values match {
    case Seq(i: Int)  ⇒ Some(JsNumber(i))
    case Seq(l: Long) ⇒ Some(JsNumber(l))
    case _            ⇒ None
  }

  override val swaggerType: JsObject = Json.obj("type" → "integer")

  override def elasticType(attributeName: String): LongFieldDefinition = field(attributeName, LongType)
}

case class EnumerationAttributeFormat[T <: Enumeration](enum: T)(implicit tag: ClassTag[T], format: Format[T#Value])
    extends AttributeFormat[T#Value](s"enumeration") {

  override def checkJson(subNames: Seq[String], value: JsValue): Or[JsValue, One[InvalidFormatAttributeError]] = value match {
    case JsString(v) if subNames.isEmpty ⇒ try { enum.withName(v); Good(value) } catch { case _: Throwable ⇒ Bad(One(InvalidFormatAttributeError("", name, JsonInputValue(value)))) }
    case _                               ⇒ Bad(One(InvalidFormatAttributeError("", name, JsonInputValue(value))))
  }

  override def fromInputValue(subNames: Seq[String], value: InputValue): T#Value Or Every[AttributeError] = {
    if (subNames.nonEmpty)
      Bad(One(InvalidFormatAttributeError("", name, value)))
    else
      value match {
        case StringInputValue(Seq(v))    ⇒ try { Good(enum.withName(v)) } catch { case _: Throwable ⇒ Bad(One(InvalidFormatAttributeError("", name, value))) }
        case JsonInputValue(JsString(v)) ⇒ try { Good(enum.withName(v)) } catch { case _: Throwable ⇒ Bad(One(InvalidFormatAttributeError("", name, value))) }
        case _                           ⇒ Bad(One(InvalidFormatAttributeError("", name, value)))
      }
  }

  override def elasticToJson(values: Seq[Any]): Option[JsValue] = values match {
    case Seq(s: String) ⇒ Try(enum.withName(s)).toOption.map(_ ⇒ JsString(s))
    case _              ⇒ None
  }

  override def swaggerType = JsObject(Seq("type" → JsString("string"), "enum" → JsArray(enum.values.map(v ⇒ JsString(v.toString)).toSeq)))

  override def elasticType(attributeName: String): StringFieldDefinition = field(attributeName, StringType) index "not_analyzed"
}

case class ListEnumeration(enumerationName: String)(dblists: DBLists) extends AttributeFormat[String](s"enumeration") {
  def items: Set[String] = dblists("list_" + enumerationName).cachedItems.map(_.mapTo[String]).toSet //getItems[String].map(_.map(_._2).toSet)
  override def checkJson(subNames: Seq[String], value: JsValue): Or[JsValue, One[InvalidFormatAttributeError]] = value match {
    case JsString(v) if subNames.isEmpty && items.contains(v) ⇒ Good(value)
    case _                                                    ⇒ Bad(One(InvalidFormatAttributeError("", name, JsonInputValue(value))))
  }

  override def fromInputValue(subNames: Seq[String], value: InputValue): String Or Every[AttributeError] = {
    if (subNames.nonEmpty)
      Bad(One(InvalidFormatAttributeError("", name, value)))
    else
      value match {
        case StringInputValue(Seq(v)) if items.contains(v)    ⇒ Good(v)
        case JsonInputValue(JsString(v)) if items.contains(v) ⇒ Good(v)
        case _                                                ⇒ Bad(One(InvalidFormatAttributeError("", name, value)))
      }
  }

  override def elasticToJson(values: Seq[Any]): Option[JsValue] = values match {
    case Seq(s: String) if items.contains(s) ⇒ Some(JsString(s))
    case _                                   ⇒ None
  }

  override def swaggerType = JsObject(Seq("type" → JsString("string"), "enum" → JsArray(items.toSeq.map(JsString))))

  override def elasticType(attributeName: String): StringFieldDefinition = field(attributeName, StringType) index "not_analyzed"
}

object UUIDAttributeFormat extends AttributeFormat[UUID]("uuid") {
  override def checkJson(subNames: Seq[String], value: JsValue): Or[JsValue, One[InvalidFormatAttributeError]] = value match {
    case JsString(v) if subNames.isEmpty ⇒ try { UUID.fromString(v); Good(value) } catch { case _: Throwable ⇒ Bad(One(InvalidFormatAttributeError("", name, JsonInputValue(value)))) }
    case _                               ⇒ Bad(One(InvalidFormatAttributeError("", name, JsonInputValue(value))))
  }

  override def fromInputValue(subNames: Seq[String], value: InputValue): UUID Or Every[AttributeError] = {
    if (subNames.nonEmpty)
      Bad(One(InvalidFormatAttributeError("", name, value)))
    else
      value match {
        case StringInputValue(Seq(v))    ⇒ try { Good(UUID.fromString(v)) } catch { case _: Throwable ⇒ Bad(One(InvalidFormatAttributeError("", name, value))) }
        case JsonInputValue(JsString(v)) ⇒ try { Good(UUID.fromString(v)) } catch { case _: Throwable ⇒ Bad(One(InvalidFormatAttributeError("", name, value))) }
        case _                           ⇒ Bad(One(InvalidFormatAttributeError("", name, value)))
      }
  }

  override def elasticToJson(values: Seq[Any]): Option[JsValue] = values match {
    case Seq(s: String) ⇒ Try(UUID.fromString(s)).toOption.map(_ ⇒ JsString(s))
    case _              ⇒ None
  }

  override val swaggerType: JsObject = Json.obj("type" → "string")

  override def elasticType(attributeName: String): StringFieldDefinition = field(attributeName, StringType) index "not_analyzed"
}

object HashAttributeFormat extends AttributeFormat[String]("hash") {
  val validDigits = "0123456789abcdefABCDEF"

  override def checkJson(subNames: Seq[String], value: JsValue): Or[JsValue, One[InvalidFormatAttributeError]] = value match {
    case JsString(v) if subNames.isEmpty && v.forall(c ⇒ validDigits.contains(c)) ⇒ Good(value)
    case _ ⇒ Bad(One(InvalidFormatAttributeError("", name, JsonInputValue(value))))
  }

  override def fromInputValue(subNames: Seq[String], value: InputValue): String Or Every[AttributeError] = {
    if (subNames.nonEmpty)
      Bad(One(InvalidFormatAttributeError("", name, value)))
    else
      value match {
        case StringInputValue(Seq(v)) if v.forall(c ⇒ validDigits.contains(c)) ⇒ Good(v.toLowerCase)
        case JsonInputValue(JsString(v)) if v.forall(c ⇒ validDigits.contains(c)) ⇒ Good(v.toLowerCase)
        case _ ⇒ Bad(One(InvalidFormatAttributeError("", name, value)))
      }
  }

  override def elasticToJson(values: Seq[Any]): Option[JsValue] = values match {
    case Seq(s: String) if s.forall(c ⇒ validDigits.contains(c)) ⇒ Some(JsString(s))
    case _                                                       ⇒ None
  }

  override val swaggerType: JsObject = Json.obj("type" → "string")

  override def elasticType(attributeName: String): StringFieldDefinition = field(attributeName, StringType) index "not_analyzed"
}

object AttachmentAttributeFormat extends AttributeFormat[Attachment]("attachment") {
  override def checkJson(subNames: Seq[String], value: JsValue): Or[JsValue, One[InvalidFormatAttributeError]] = {
    lazy val validJson = fileInputValueFormat.reads(value).asOpt orElse jsFormat.reads(value).asOpt
    if (subNames.isEmpty && validJson.isDefined)
      Good(value)
    else
      Bad(One(InvalidFormatAttributeError("", name, JsonInputValue(value))))
  }

  val forbiddenChar = Seq('/', '\n', '\r', '\t', '\u0000', '\f', '`', '?', '*', '\\', '<', '>', '|', '\"', ':', ';')

  override def inputValueToJson(subNames: Seq[String], value: InputValue): JsValue Or Every[AttributeError] = {
    if (subNames.nonEmpty)
      Bad(One(InvalidFormatAttributeError("", name, value)))
    else
      value match {
        case fiv: FileInputValue if fiv.name.intersect(forbiddenChar).isEmpty ⇒ Good(Json.toJson(fiv)(fileInputValueFormat))
        case aiv: AttachmentInputValue ⇒ Good(Json.toJson(aiv.toAttachment)(jsFormat))
        case _ ⇒ Bad(One(InvalidFormatAttributeError("", name, value)))
      }
  }

  override def fromInputValue(subNames: Seq[String], value: InputValue): Attachment Or Every[AttributeError] =
    Bad(One(InvalidFormatAttributeError("", name, value)))

  override def elasticToJson(values: Seq[Any]): Option[JsValue] = values match {
    case Seq(m: util.Map[_, _]) ⇒
      for {
        name ← Option(m.get("name")).flatMap(n ⇒ StringAttributeFormat.elasticToJson(Seq(n)))
        hashes ← Option(m.get("hashes")).flatMap(h ⇒ HashAttributeFormat.elasticToJson(Seq(h)))
        size ← Option(m.get("size")).flatMap(s ⇒ NumberAttributeFormat.elasticToJson(Seq(s)))
        contentType ← Option(m.get("contentType")).flatMap(ct ⇒ StringAttributeFormat.elasticToJson(Seq(ct)))
        id ← Option(m.get("id")).flatMap(i ⇒ StringAttributeFormat.elasticToJson(Seq(i)))
      } yield JsObject(Seq(
        "name" → name,
        "hashes" → hashes,
        "size" → size,
        "contentType" → contentType,
        "id" → id))
    case _ ⇒ None
  }

  override val swaggerType: JsObject = Json.obj("type" → "File", "required" → true) // swagger bug : File input must be required
  override def elasticType(attributeName: String): NestedFieldDefinition = field(attributeName, NestedType) as (
    field("name", StringType) index "not_analyzed",
    field("hashes", StringType) index "not_analyzed",
    field("size", LongType),
    field("contentType", StringType),
    field("id", StringType))
}

case class ObjectAttributeFormat(subAttributes: Seq[Attribute[_]]) extends AttributeFormat[JsObject]("nested") {
  lazy val log = Logger(getClass)

  override def checkJson(subNames: Seq[String], value: JsValue): JsObject Or Every[AttributeError] = ???

  override def checkJsonForCreation(subNames: Seq[String], value: JsValue): JsObject Or Every[AttributeError] = {
    value match {
      case obj: JsObject if subNames.isEmpty ⇒
        subAttributes.validatedBy { attr ⇒
          attr.validateForCreation((value \ attr.name).asOpt[JsValue])
        }
          .map { _ ⇒ obj }
      case _ ⇒ Bad(One(InvalidFormatAttributeError("", name, JsonInputValue(value))))
    }
  }

  override def checkJsonForUpdate(subNames: Seq[String], value: JsValue): JsObject Or Every[AttributeError] = {
    value match {
      case obj: JsObject if subNames.isEmpty ⇒
        obj.fields.validatedBy {
          case (name, v) ⇒
            subAttributes
              .find(_.name == name)
              .map(_.validateForUpdate(subNames, v))
              .getOrElse(Bad(One(UnknownAttributeError(name, v))))
        }
          .map { _ ⇒ obj }
      case _ ⇒ Bad(One(InvalidFormatAttributeError("", name, JsonInputValue(value))))
    }
  }

  override def fromInputValue(subNames: Seq[String], value: InputValue): JsObject Or Every[AttributeError] = {
    subNames
      .headOption
      .map { subName ⇒
        subAttributes
          .find(_.name == subName)
          .map { subAttribute ⇒
            value.jsonValue match {
              case jsvalue @ (JsNull | JsArray(Nil)) ⇒ Good(JsObject(Seq(subName → jsvalue)))
              case _ ⇒ subAttribute.format.inputValueToJson(subNames.tail, value)
                .map(v ⇒ JsObject(Seq(subName → v)))
                .badMap { errors ⇒ errors.map(e ⇒ e.withName(name + "." + e.name)) }
            }
          }
          .getOrElse(Bad(One(UnknownAttributeError(name, value.jsonValue))))
      }
      .getOrElse {
        value match {
          case JsonInputValue(v: JsObject) ⇒
            v.fields
              .validatedBy {
                case (_, jsvalue) if jsvalue == JsNull || jsvalue == JsArray(Nil) ⇒ Good(jsvalue)
                case (_name, jsvalue) ⇒
                  subAttributes.find(_.name == _name)
                    .map(_.format.fromInputValue(Nil, JsonInputValue(jsvalue)))
                    .getOrElse(Bad(One(UnknownAttributeError(_name, Json.toJson(value)))))
              }
              .map { _ ⇒ v }
          case _ ⇒ Bad(One(InvalidFormatAttributeError("", name, value)))
        }
      }
  }

  override def elasticToJson(values: Seq[Any]): Option[JsValue] = ???

  override val swaggerType: JsObject = Json.obj("type" → "string")

  override def elasticType(attributeName: String): NestedFieldDefinition = field(attributeName, NestedType) as (subAttributes.map(_.elasticMapping): _*)
}

//object ObjectAttributeFormat extends AttributeFormat[JsObject]("object") {
//  def fromInputValue(value: InputValue): JsObject Or Every[InvalidFormatAttributeError] = value match {
//    case StringInputValue(Seq(v))    => try { Good(Json.fromInputValue(v).as[JsObject]) } catch { case _: Throwable => Bad(One(InvalidFormatAttributeError("", name, value))) }
//    case JsonInputValue(v: JsObject) => Good(v)
//    case _                           => Bad(One(InvalidFormatAttributeError("", name, value)))
//  }
//  def toJson(values: Seq[Any]): Option[JsValue] = ???
//  //values match { // FIXME
//  //    case Seq(s: String) => Some(JsString(s))
//  //    case _              => None
//  //  }
//  val swaggerType = Json.obj("type" -> "string")
//  def elasticType(attributeName: String) = field(attributeName, ObjectType)
//}

object BinaryAttributeFormat extends AttributeFormat[Array[Byte]]("binary")(binaryFormats) {
  override def checkJson(subNames: Seq[String], value: JsValue) = Bad(One(InvalidFormatAttributeError("", name, JsonInputValue(value))))

  override def fromInputValue(subNames: Seq[String], value: InputValue): Array[Byte] Or Every[AttributeError] = sys.error("not supported")

  override def elasticToJson(values: Seq[Any]): Option[JsValue] = values match {
    case Seq(s: String) ⇒ Some(JsString(s))
    case _              ⇒ None
  }

  override def swaggerType: Nothing = sys.error("not supported")

  override def elasticType(attributeName: String): BinaryFieldDefinition = field(attributeName, BinaryType)
}

object MetricsAttributeFormat extends AttributeFormat[JsValue]("metrics") {
  override def checkJson(subNames: Seq[String], value: JsValue): Or[JsValue, Every[AttributeError]] = fromInputValue(subNames, JsonInputValue(value))

  override def fromInputValue(subNames: Seq[String], value: InputValue): JsValue Or Every[AttributeError] = {
    if (subNames.isEmpty) {
      value match {
        case JsonInputValue(v: JsObject) ⇒
          v.fields
            .validatedBy {
              case (_, _: JsNumber) ⇒ Good(())
              case (_, JsNull)      ⇒ Good(())
              case _                ⇒ Bad(One(InvalidFormatAttributeError("", name, value)))
            }
            .map(_ ⇒ v)
        case _ ⇒ Bad(One(InvalidFormatAttributeError("", name, value)))
      }
    }
    else {
      OptionalAttributeFormat(NumberAttributeFormat).inputValueToJson(subNames.tail, value) //.map(v => JsObject(Seq(subNames.head -> v)))
    }
  }

  override def elasticToJson(values: Seq[Any]): Option[JsValue] = ???

  //values match { // FIXME
  //    case Seq(s: String) => Some(JsString(s))
  //    case _              => None
  //  }
  override val swaggerType: JsObject = Json.obj("type" → "string")

  override def elasticType(attributeName: String): ObjectFieldDefinition = field(attributeName, ObjectType).as(field("_default_", LongType))
}

case class MultiAttributeFormat[T](attributeFormat: AttributeFormat[T]) extends AttributeFormat[Seq[T]]("multi-" + attributeFormat.name)(multiFormat(attributeFormat.jsFormat)) { // {//}(Format(Reads.seq(attributeFormat.jsFormat), Writes.seq(attributeFormat.jsFormat))) {
  override def checkJsonForCreation(subNames: Seq[String], value: JsValue): Or[JsArray, Every[AttributeError]] = value match {
    case JsArray(values) if subNames.isEmpty ⇒ values.validatedBy(v ⇒ attributeFormat.checkJsonForCreation(Nil, v)).map(JsArray)
    case _                                   ⇒ Bad(One(InvalidFormatAttributeError("", name, JsonInputValue(value))))
  }

  override def checkJsonForUpdate(subNames: Seq[String], value: JsValue): Or[JsArray, Every[AttributeError]] = value match {
    case JsArray(values) if subNames.isEmpty ⇒ values.validatedBy(v ⇒ attributeFormat.checkJsonForUpdate(Nil, v)).map(JsArray)
    case _                                   ⇒ Bad(One(InvalidFormatAttributeError("", name, JsonInputValue(value))))
  }

  override def checkJson(subNames: Seq[String], value: JsValue): Or[JsArray, Every[AttributeError]] = value match {
    case JsArray(values) if subNames.isEmpty ⇒ values.validatedBy(v ⇒ attributeFormat.checkJsonForUpdate(Nil, v)).map(JsArray)
    case _                                   ⇒ Bad(One(InvalidFormatAttributeError("", name, JsonInputValue(value))))
  }

  override def inputValueToJson(subNames: Seq[String], value: InputValue): JsValue Or Every[AttributeError] = value match {
    case JsonInputValue(JsArray(xs)) ⇒ xs.map(x ⇒ JsonInputValue(x)).validatedBy(i ⇒ attributeFormat.inputValueToJson(subNames, i)).map(JsArray)
    case StringInputValue(xs)        ⇒ xs.filterNot(_.isEmpty).map(x ⇒ StringInputValue(x :: Nil)).validatedBy(i ⇒ attributeFormat.inputValueToJson(subNames, i)).map(JsArray)
    case _                           ⇒ Bad(One(InvalidFormatAttributeError("", name, value)))
  }

  override def fromInputValue(subNames: Seq[String], value: InputValue): Seq[T] Or Every[AttributeError] = value match {
    case JsonInputValue(JsArray(xs)) ⇒ xs.map(JsonInputValue).validatedBy(i ⇒ attributeFormat.fromInputValue(subNames, i))
    case StringInputValue(xs)        ⇒ xs.filterNot(_.isEmpty).map(x ⇒ StringInputValue(x :: Nil)).validatedBy(i ⇒ attributeFormat.fromInputValue(subNames, i))
    case _                           ⇒ Bad(One(InvalidFormatAttributeError("", name, value)))
  }

  override def elasticToJson(values: Seq[Any]): Option[JsValue] = values match {
    case xs: Seq[_] ⇒ xs.foldLeft[Option[JsArray]](Some(JsArray())) {
      case (Some(arr), x) ⇒ attributeFormat.elasticToJson(Seq(x)).fold[Option[JsArray]](None)(j ⇒ Some(arr :+ j))
      case (None, _)      ⇒ None
    }
    case _ ⇒ None
  }

  override def swaggerType: JsObject = attributeFormat.swaggerType // TODO Add array
  override def elasticType(attributeName: String): TypedFieldDefinition = attributeFormat.elasticType(attributeName)
}

case class OptionalAttributeFormat[T](attributeFormat: AttributeFormat[T]) extends AttributeFormat[Option[T]](attributeFormat.name)(optionFormat(attributeFormat.jsFormat)) { //}(Format(Reads.seq(attributeFormat.jsFormat), Writes.seq(attributeFormat.jsFormat))) {
  override def checkJson(subNames: Seq[String], value: JsValue): Or[JsValue, Every[AttributeError]] = value match {
    //case _ if !subNames.isEmpty => Bad(One(InvalidFormatAttributeError("", name, JsonInputValue(value))))
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

  override def elasticToJson(values: Seq[Any]): Option[JsValue] = attributeFormat.elasticToJson(values)

  override def swaggerType: JsObject = attributeFormat.swaggerType // TODO Add array
  override def elasticType(attributeName: String): TypedFieldDefinition = attributeFormat.elasticType(attributeName)
}

object AttributeFormat {
  val dateFmt = DateAttributeFormat
  val textFmt = TextAttributeFormat
  val stringFmt = StringAttributeFormat
  val booleanFmt = BooleanAttributeFormat
  val numberFmt = NumberAttributeFormat

  def enumFmt[T <: Enumeration](e: T)(implicit tag: ClassTag[T], format: Format[T#Value]): EnumerationAttributeFormat[T] = EnumerationAttributeFormat[T](e)

  val uuidFmt = UUIDAttributeFormat
  val hashFmt = HashAttributeFormat
  val binaryFmt = BinaryAttributeFormat

  def listEnumFmt(enumerationName: String)(dblists: DBLists): ListEnumeration = ListEnumeration(enumerationName)(dblists)

  val attachmentFmt = AttachmentAttributeFormat
  val metricsFmt = MetricsAttributeFormat

  def objectFmt(subAttributes: Seq[Attribute[_]]) = ObjectAttributeFormat(subAttributes)
}

object AttributeOption extends Enumeration with HiveEnumeration {
  type Type = Value
  val readonly, unaudited, model, form, sensitive, user = Value
}

case class Attribute[T](
    modelName: String,
    name: String,
    format: AttributeFormat[T],
    options: Seq[AttributeOption.Type],
    defaultValue: Option[() ⇒ T],
    description: String) {
  def defaultValueJson: Option[JsValue] = defaultValue.map(d ⇒ format.jsFormat.writes(d()))

  def toOptional: Attribute[Option[T]] =
    Attribute[Option[T]](
      modelName,
      name,
      OptionalAttributeFormat(format),
      options,
      defaultValue.map(a ⇒ () ⇒ Some(a())),
      description)

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
  lazy val isUser: Boolean = options.contains(AttributeOption.user)

  def elasticMapping: TypedFieldDefinition = format.elasticType(name) match {
    case a: attributes.AttributeIndex if isSensitive ⇒ a index "no"
    case a                                           ⇒ a
  }

  def validateForCreation(value: Option[JsValue]): Option[JsValue] Or Every[AttributeError] = {
    value match {
      case Some(JsNull) if !isRequired       ⇒ Good(value)
      case Some(JsArray(Nil)) if !isRequired ⇒ Good(value)
      case None if !isRequired               ⇒ Good(value)
      case Some(JsNull) | Some(JsArray(Nil)) | None ⇒
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
  }

  def validateForUpdate(subNames: Seq[String], value: JsValue): JsValue Or Every[AttributeError] = {
    value match {
      case _ if isReadonly                     ⇒ Bad(One(UpdateReadOnlyAttributeError(name)))
      case JsNull | JsArray(Nil) if isRequired ⇒ Bad(One(MissingAttributeError(name)))
      case JsNull | JsArray(Nil)               ⇒ Good(value)
      case v ⇒
        format.checkJsonForUpdate(subNames, v).badMap(_.map {
          case ifae: InvalidFormatAttributeError ⇒ ifae.copy(name = name)
          case other                             ⇒ other
        })
    }
  }
}
