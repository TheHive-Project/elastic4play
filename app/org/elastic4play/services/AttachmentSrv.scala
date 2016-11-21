package org.elastic4play.services

import java.nio.file.Files

import javax.inject.{ Inject, Singleton }

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.DurationInt

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{ FileIO, Sink, Source, StreamConverters }
import akka.util.ByteString

import play.api.Configuration
import play.api.libs.json.{ JsArray, JsNull, JsObject, JsValue }
import play.api.libs.json.JsValue.jsValueToJsLookup
import play.api.libs.json.Json
import play.api.libs.json.Json.toJsFieldJsValueWrapper

import org.elastic4play.{ AttributeCheckingError, InvalidFormatAttributeError, MissingAttributeError }
import org.elastic4play.controllers.FileInputValue
import org.elastic4play.controllers.JsonFormat.fileInputValueFormat
import org.elastic4play.controllers.JsonInputValue
import org.elastic4play.database.DBCreate
import org.elastic4play.models.{ AttributeDef, AttributeFormat ⇒ F, BaseModelDef, EntityDef, ModelDef }
import org.elastic4play.services.JsonFormat.attachmentFormat
import org.elastic4play.utils.{ Hash, Hasher }

case class Attachment(name: String, hashes: Seq[Hash], size: Long, contentType: String, id: String)
object Attachment {
  def apply(id: String, hashes: Seq[Hash], fiv: FileInputValue): Attachment = Attachment(fiv.name, hashes, Files.size(fiv.filepath), fiv.contentType, id)
}

trait AttachmentAttributes { _: AttributeDef ⇒
  val data = attribute("binary", F.binaryFmt, "data")
}

@Singleton
class AttachmentModel(datastoreName: String) extends ModelDef[AttachmentModel, AttachmentChunk](datastoreName) with AttachmentAttributes {
  @Inject() def this(configuration: Configuration) = this(configuration.getString("datastore.name").get)
}
class AttachmentChunk(model: AttachmentModel, attributes: JsObject) extends EntityDef[AttachmentModel, AttachmentChunk](model, attributes) with AttachmentAttributes

@Singleton
class AttachmentSrv(
    mainHash: String,
    extraHashes: Seq[String],
    chunkSize: Int,
    dbCreate: DBCreate,
    getSrv: GetSrv,
    attachmentModel: AttachmentModel,
    implicit val ec: ExecutionContext,
    implicit val mat: Materializer) {

  @Inject() def this(configuration: Configuration, dbCreate: DBCreate,
    getSrv: GetSrv,
    attachmentModel: AttachmentModel,
    ec: ExecutionContext,
    mat: Materializer) =
    this(
      configuration.getString("datastore.hash.main").get,
      configuration.getStringSeq("datastore.hash.extra").get,
      configuration.getBytes("datastore.chunksize").get.toInt,
      dbCreate,
      getSrv,
      attachmentModel,
      ec,
      mat)

  val mainHasher = Hasher(mainHash)
  val extraHashers = Hasher(mainHash +: extraHashes: _*)

  /**
   * Handles attachments : send to datastore and build an object with hash and filename
   */
  def apply(model: BaseModelDef)(attributes: JsObject): Future[JsObject] = {
    // find all declared attribute as attachment in submitted data
    model.attachmentAttributes.foldLeft(Future.successful(attributes)) {
      case (attributes, (name, isRequired)) ⇒
        attributes.flatMap { a ⇒
          // try to convert in FileInputValue Scala Object
          (a \ name).asOpt[FileInputValue] match {
            case Some(attach) ⇒
              // save attachment and replace FileInputValue json representation to JsObject containing attachment attributes
              save(attach).map { attachment ⇒
                a - name + (name → Json.toJson(attachment))
              }
            // if conversion to FileInputValue fails, it means that attribute is missing of format is invalid
            case _ ⇒ (a \ name).asOpt[JsValue] match {
              case Some(v) if v != JsNull && v != JsArray(Nil) ⇒
                Future.failed(AttributeCheckingError(model.name, Seq(
                  InvalidFormatAttributeError(name, "attachment", (a \ name).asOpt[FileInputValue].getOrElse(JsonInputValue((a \ name).as[JsValue]))))))
              case _ ⇒
                if (isRequired)
                  Future.failed(AttributeCheckingError(model.name, Seq(MissingAttributeError(name))))
                else
                  Future.successful(a)
            }
          }
        }
    }
  }

  def save(fiv: FileInputValue): Future[Attachment] = {
    for {
      hash ← mainHasher.fromPath(fiv.filepath).map(_.head.toString)
      hashes ← extraHashers.fromPath(fiv.filepath)
      attachment ← getSrv[AttachmentModel, AttachmentChunk](attachmentModel, hash + "_0", Some(Nil))
        .map { _ ⇒ Attachment(hash, hashes, fiv) }
        .fallbackTo { // it it doesn't exist, create it
          FileIO.fromPath(fiv.filepath, chunkSize)
            .zip(Source.fromIterator { () ⇒ Iterator.iterate(0)(_ + 1) })
            .mapAsync(5) {
              case (buffer, index) ⇒
                val data = java.util.Base64.getEncoder.encodeToString(buffer.toArray)
                dbCreate(attachmentModel.name, None, Json.obj("binary" → data, "_id" → s"${hash}_${index}"))
            }
            .runWith(Sink.ignore)
            .map { _ ⇒ Attachment(hash, hashes, fiv) }
        }
    } yield attachment
  }

  def source(id: String): Source[ByteString, NotUsed] =
    Source.unfoldAsync(0) { chunkNumber ⇒
      getSrv[AttachmentModel, AttachmentChunk](attachmentModel, s"${id}_${chunkNumber}", Some(Seq(attachmentModel.data)))
        .map { entity ⇒ Some((chunkNumber + 1, ByteString(entity.data()))) }
        .recover { case _ ⇒ None }
    }

  def stream(id: String) = source(id).runWith(StreamConverters.asInputStream(1.minute))

  def getHashes(id: String): Future[Seq[Hash]] = extraHashers.fromSource(source(id))
}
