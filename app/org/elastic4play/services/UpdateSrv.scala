package org.elastic4play.services

import java.util.Date

import javax.inject.{ Inject, Singleton }

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

import play.api.libs.json.{ JsObject, Json }
import play.api.libs.json.Json.toJsFieldJsValueWrapper

import org.elastic4play.AttributeCheckingError
import org.elastic4play.JsonFormat.dateFormat
import org.elastic4play.UnknownAttributeError
import org.elastic4play.controllers.Fields
import org.elastic4play.database.DBModify
import org.elastic4play.models.{ AbstractModelDef, BaseEntity, BaseModelDef, EntityDef }
import org.elastic4play.utils.{ RichFuture, RichOr }
import org.scalactic.Accumulation.convertGenTraversableOnceToValidatable
import org.scalactic.Bad
import org.scalactic.Every.everyToGenTraversableOnce
import org.scalactic.One

@Singleton
class UpdateSrv @Inject() (
    fieldsSrv: FieldsSrv,
    dbModify: DBModify,
    getSrv: GetSrv,
    attachmentSrv: AttachmentSrv,
    eventSrv: EventSrv,
    implicit val ec: ExecutionContext) {

  /**
   * Check if entity attributes are valid. Format is not checked as it has been already checked.
   */
  private[services] def checkAttributes(attrs: JsObject, model: BaseModelDef): Future[JsObject] = {
    attrs.fields
      .map {
        case (name, value) ⇒
          val names = name.split("\\.")
          (name, names, value, model.modelAttributes.get(names.head))
      }
      .validatedBy {
        case (name, names, value, None)       ⇒ Bad(One(UnknownAttributeError(name, value)))
        case (name, names, value, Some(attr)) ⇒ attr.validateForUpdate(names.tail, value).map(name → _)
      }
      .fold(attrs ⇒ Future.successful(JsObject(attrs)), errors ⇒ Future.failed(AttributeCheckingError(model.name, errors)))
  }

  private[services] def doUpdate[E <: BaseEntity](entity: E, attributes: JsObject)(implicit authContext: AuthContext): Future[E] = {
    for {
      attributesAfterHook ← entity.model.updateHook(entity, addMetaFields(attributes))
      checkedAttributes ← checkAttributes(attributesAfterHook, entity.model)
      attributesWithAttachment ← attachmentSrv(entity.model)(checkedAttributes)
      newEntity ← dbModify(entity, attributesWithAttachment)
    } yield newEntity.asInstanceOf[E]
  }

  private[services] def addMetaFields(attrs: JsObject)(implicit authContext: AuthContext): JsObject =
    attrs ++
      Json.obj(
        "updatedBy" → authContext.userId,
        "updatedAt" → Json.toJson(new Date))

  private[services] def removeMetaFields(attrs: JsObject): JsObject = attrs - "updatedBy" - "updatedAt"

  def apply[M <: AbstractModelDef[M, E], E <: EntityDef[M, E]](model: M, id: String, fields: Fields)(implicit authContext: AuthContext): Future[E] = {
    for {
      entity ← getSrv[M, E](model, id)
      newEntity ← apply[E](entity, fields)
    } yield newEntity
  }

  def apply[M <: AbstractModelDef[M, E], E <: EntityDef[M, E]](model: M, ids: Seq[String], fields: Fields)(implicit authContext: AuthContext): Future[Seq[Try[E]]] = {
    Future.sequence {
      ids.map { id ⇒
        getSrv[M, E](model, id)
          .flatMap(entity ⇒ apply[E](entity, fields).toTry)
      }
    }
  }

  def apply[E <: BaseEntity](entity: E, fields: Fields)(implicit authContext: AuthContext): Future[E] = {
    for {
      attributes ← fieldsSrv.parse(fields, entity.model).toFuture
      newEntity ← doUpdate(entity, attributes)
      _ = eventSrv.publish(AuditOperation(newEntity, AuditableAction.Update, removeMetaFields(attributes), authContext))
    } yield newEntity
  }

  def apply[E <: BaseEntity](entitiesAttributes: Seq[(E, Fields)])(implicit authContext: AuthContext): Future[Seq[Try[E]]] = {
    Future.sequence(entitiesAttributes.map {
      case (entity, fields) ⇒ apply(entity, fields).toTry
    })
  }
}