package org.elastic4play.services

import javax.inject.{ Inject, Singleton }

import scala.concurrent.{ ExecutionContext, Future }

import akka.stream.Materializer
import akka.stream.scaladsl.{ Sink, Source }

import play.api.Logger
import play.api.libs.json.{ JsObject, Json, JsString }

import org.elastic4play.InternalError
import org.elastic4play.database.DBConfiguration
import org.elastic4play.models.{ AttributeOption, BaseEntity, ChildModelDef }
import org.elastic4play.models.JsonFormat.baseModelEntityWrites

@Singleton
class AuxSrv @Inject() (
    db: DBConfiguration,
    findSrv: FindSrv,
    modelSrv: ModelSrv,
    implicit val ec: ExecutionContext,
    implicit val mat: Materializer) {
  import QueryDSL._
  val log = Logger(getClass)

  def removeUnauditedAttributes(entity: BaseEntity): JsObject = {
    JsObject(
      entity.attributes.fields
        .map { case (name, value) ⇒ (name, value, entity.model.attributes.find(_.name == name)) }
        .collect { case (name, value, Some(desc)) if !desc.options.contains(AttributeOption.unaudited) ⇒ name → value }) +
      ("id" → JsString(entity.id)) +
      ("type" → JsString(entity.model.name))
  }
  def apply(entity: BaseEntity, nparent: Int, withStats: Boolean, removeUnaudited: Boolean): Future[JsObject] = {
    val entityWithParent = entity.model match {
      case childModel: ChildModelDef[_, _, _, _] if nparent > 0 ⇒
        val (src, total) = findSrv(childModel.parentModel, ("_id" ~= entity.parentId.getOrElse(throw InternalError(s"Child entity $entity has no parent ID"))), Some("0-1"), Nil)
        src
          .mapAsync(1) { parent ⇒
            apply(parent, nparent - 1, withStats, removeUnaudited).map { parent ⇒
              val entityObj = removeUnaudited match {
                case true  ⇒ removeUnauditedAttributes(entity)
                case false ⇒ Json.toJson(entity).as[JsObject]
              }
              entityObj + (childModel.parentModel.name → parent)
            }
          }
          .runWith(Sink.headOption)
          .map(_.getOrElse {
            log.warn(s"Child entity (${childModel.name} ${entity.id}) has no parent !")
            JsObject(Nil)
          })
      case _ if removeUnaudited ⇒ Future.successful(removeUnauditedAttributes(entity))
      case _                    ⇒ Future.successful(Json.toJson(entity).as[JsObject])
    }
    if (withStats) {
      for {
        e ← entityWithParent
        s ← entity.model.getStats(entity)
      } yield e + ("stats" → s)
    }
    else entityWithParent
  }

  def apply[A](entities: Source[BaseEntity, A], nparent: Int, withStats: Boolean, removeUnaudited: Boolean): Source[JsObject, A] = {
    entities.mapAsync(5) { entity ⇒ apply(entity, nparent, withStats, removeUnaudited) }
  }

  def apply(modelName: String, entityId: String, nparent: Int, withStats: Boolean, removeUnaudited: Boolean): Future[JsObject] = {
    if (entityId == "")
      return Future.successful(JsObject(Nil))
    modelSrv(modelName)
      .map { model ⇒
        val (src, total) = findSrv(model, ("_id" ~= entityId), Some("0-1"), Nil)
        src.mapAsync(1) { entity ⇒ apply(entity, nparent, withStats, removeUnaudited) }
          .runWith(Sink.headOption)
          .map(_.getOrElse {
            log.warn(s"Entity $modelName $entityId not found")
            JsObject(Nil)
          })
      }
      .getOrElse(Future.failed(InternalError(s"Model $modelName not found")))
  }
}