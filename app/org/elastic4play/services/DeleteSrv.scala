package org.elastic4play.services

import javax.inject.{ Inject, Singleton }

import scala.concurrent.{ ExecutionContext, Future }
import play.api.libs.json.JsObject
import org.elastic4play.NotFoundError
import org.elastic4play.database.{ DBRemove, ModifyConfig }
import org.elastic4play.models.{ AbstractModelDef, BaseEntity, EntityDef }

import scala.util.Success

@Singleton
class DeleteSrv @Inject() (
    updateSrv: UpdateSrv,
    getSrv: GetSrv,
    dbremove: DBRemove,
    eventSrv: EventSrv,
    implicit val ec: ExecutionContext) {

  def apply[M <: AbstractModelDef[M, E], E <: EntityDef[M, E]](model: M, id: String)(implicit authContext: AuthContext): Future[E] = {
    getSrv[M, E](model, id).flatMap(entity ⇒ apply(entity))
  }

  def apply[E <: BaseEntity](entity: E)(implicit authContext: AuthContext) = {
    updateSrv.doUpdate(entity, entity.model.removeAttribute, ModifyConfig.default)
      .andThen {
        case Success(newEntity) ⇒ eventSrv.publish(AuditOperation(newEntity, AuditableAction.Delete, JsObject.empty, authContext))
      }
  }

  def realDelete[M <: AbstractModelDef[M, E], E <: EntityDef[M, E]](model: M, id: String)(implicit authContext: AuthContext): Future[Unit] = {
    getSrv[M, E](model, id).flatMap(entity ⇒ realDelete(model, entity))
  }

  def realDelete[M <: AbstractModelDef[M, E], E <: EntityDef[M, E]](model: M, entity: E)(implicit authContext: AuthContext): Future[Unit] = {
    dbremove(model, entity).map { isFound ⇒
      if (isFound) eventSrv.publish(AuditOperation(entity, AuditableAction.Delete, JsObject.empty, authContext))
      else throw NotFoundError(s"${model.modelName} ${entity.id} not found")
    }
  }
}