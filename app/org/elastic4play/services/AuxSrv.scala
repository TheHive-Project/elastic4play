package org.elastic4play.services

import scala.concurrent.{ ExecutionContext, Future }

import play.api.Logger
import play.api.libs.json.{ JsObject, JsString }

import akka.stream.Materializer
import akka.stream.scaladsl.{ Sink, Source }
import javax.inject.{ Inject, Singleton }

import org.elastic4play.InternalError
import org.elastic4play.database.DBConfiguration
import org.elastic4play.models.{ AttributeOption, BaseEntity, ChildModelDef }

@Singleton
class AuxSrv @Inject() (
    db: DBConfiguration,
    findSrv: FindSrv,
    modelSrv: ModelSrv,
    implicit val ec: ExecutionContext,
    implicit val mat: Materializer) {

  import org.elastic4play.services.QueryDSL._

  private[AuxSrv] lazy val logger = Logger(getClass)

  def filterAttributes(entity: BaseEntity, filter: Seq[AttributeOption.Type] ⇒ Boolean): JsObject = {
    JsObject(
      entity.attributes.fields
        .map { case (name, value) ⇒ (name, value, entity.model.attributes.find(_.attributeName == name)) }
        .collect {
          case (name, value, Some(desc)) if filter(desc.options) ⇒ name → value
          case (name, value, _) if name.startsWith("_")          ⇒ name → value
        }) +
      ("id" → JsString(entity.id)) +
      ("_type" → JsString(entity.model.modelName))
  }

  def apply(entity: BaseEntity, nparent: Int, withStats: Boolean, removeUnaudited: Boolean): Future[JsObject] = apply(entity, nparent, withStats, opts ⇒ !removeUnaudited || !opts.contains(AttributeOption.unaudited))

  def apply(entity: BaseEntity, nparent: Int, withStats: Boolean, filter: Seq[AttributeOption.Type] ⇒ Boolean): Future[JsObject] = {
    val entityWithParent = entity.model match {
      case childModel: ChildModelDef[_, _, _, _] if nparent > 0 ⇒
        val (src, _) = findSrv(childModel.parentModel, "_id" ~= entity.parentId.getOrElse(throw InternalError(s"Child entity $entity has no parent ID")), Some("0-1"), Nil)
        src
          .mapAsync(1) { parent ⇒
            apply(parent, nparent - 1, withStats, filter).map { parent ⇒
              val entityObj = filterAttributes(entity, filter)
              entityObj + (childModel.parentModel.modelName → parent)
            }
          }
          .runWith(Sink.headOption)
          .map(_.getOrElse {
            logger.warn(s"Child entity (${childModel.modelName} ${entity.id}) has no parent !")
            JsObject.empty
          })
      case _ ⇒ Future.successful(filterAttributes(entity, filter))
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
      return Future.successful(JsObject.empty)
    modelSrv(modelName)
      .map { model ⇒
        val (src, _) = findSrv(model, "_id" ~= entityId, Some("0-1"), Nil)
        src.mapAsync(1) { entity ⇒ apply(entity, nparent, withStats, removeUnaudited) }
          .runWith(Sink.headOption)
          .map(_.getOrElse {
            logger.warn(s"Entity $modelName $entityId not found")
            JsObject.empty
          })
      }
      .getOrElse(Future.failed(InternalError(s"Model $modelName not found")))
  }
}