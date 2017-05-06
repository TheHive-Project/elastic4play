package org.elastic4play

import java.util.Date

import play.api.libs.json._
import org.elastic4play.controllers.JsonFormat._

import scala.util.{ Failure, Success, Try }

object JsonFormat {
  val datePattern = "yyyyMMdd'T'HHmmssZ"
  private val dateReads: Reads[Date] = Reads.dateReads(datePattern).orElse(Reads.DefaultDateReads).orElse(Reads.LongReads.map(new Date(_)))
  private val dateWrites: Writes[Date] = Writes[Date](d ⇒ JsNumber(d.getTime))
  implicit val dateFormat: Format[Date] = Format(dateReads, dateWrites)

  private val invalidFormatAttributeErrorWrites = Json.writes[InvalidFormatAttributeError]
  private val unknownAttributeErrorWrites = Json.writes[UnknownAttributeError]
  private val updateReadOnlyAttributeErrorWrites = Json.writes[UpdateReadOnlyAttributeError]
  private val missingAttributeErrorWrites = Json.writes[MissingAttributeError]

  implicit val attributeCheckingExceptionWrites: OWrites[AttributeCheckingError] = OWrites[AttributeCheckingError] { ace ⇒
    Json.obj(
      "tableName" → ace.tableName,
      "type" → "AttributeCheckingError",
      "errors" → Json.arr(ace.errors.map {
        case e: InvalidFormatAttributeError  ⇒ invalidFormatAttributeErrorWrites.writes(e)
        case e: UnknownAttributeError        ⇒ unknownAttributeErrorWrites.writes(e)
        case e: UpdateReadOnlyAttributeError ⇒ updateReadOnlyAttributeErrorWrites.writes(e)
        case e: MissingAttributeError        ⇒ missingAttributeErrorWrites.writes(e)
      }))
  }

  implicit def tryWrites[A](implicit aWrites: Writes[A]): Writes[Try[A]] = Writes[Try[A]] {
    case Success(a) ⇒ aWrites.writes(a)
    case Failure(t) ⇒ JsString(t.getMessage)
  }
}