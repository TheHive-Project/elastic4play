package org.elastic4play

import scala.annotation.implicitNotFound
import scala.util.{ Failure, Success, Try }

import play.api.libs.json.{ Format, JsArray, JsObject, JsString, Json, Reads, Writes }

import org.elastic4play.controllers.JsonFormat.inputValueFormat

object JsonFormat {
  val datePattern = "yyyyMMdd'T'HHmmssZ"
  implicit val dateFormat = Format(Reads.dateReads(datePattern), Writes.dateWrites(datePattern))

  val invalidFormatAttributeErrorWrites = Json.writes[InvalidFormatAttributeError]
  val unknownAttributeErrorWrites = Json.writes[UnknownAttributeError]
  val updateReadOnlyAttributeErrorWrites = Json.writes[UpdateReadOnlyAttributeError]
  val missingAttributeErrorWrites = Json.writes[MissingAttributeError]

  implicit val attributeCheckingExceptionWrites = Writes[AttributeCheckingError]((ace: AttributeCheckingError) ⇒ JsObject(Seq(
    "tableName" → JsString(ace.tableName),
    "type" → JsString("AttributeCheckingError"),
    "errors" → JsArray(ace.errors.map {
      case e: InvalidFormatAttributeError  ⇒ invalidFormatAttributeErrorWrites.writes(e)
      case e: UnknownAttributeError        ⇒ unknownAttributeErrorWrites.writes(e)
      case e: UpdateReadOnlyAttributeError ⇒ updateReadOnlyAttributeErrorWrites.writes(e)
      case e: MissingAttributeError        ⇒ missingAttributeErrorWrites.writes(e)
    }))))

  implicit def tryWrites[A](implicit aWrites: Writes[A]) = Writes[Try[A]] {
    case Success(a) ⇒ aWrites.writes(a)
    case Failure(t) ⇒ JsString(t.getMessage)
  }
}