package org.elastic4play.services

import javax.inject.{ Inject, Singleton }

import scala.concurrent.{ ExecutionContext, Future }

import play.api.libs.json.JsObject

import org.elastic4play.NotFoundError
import org.elastic4play.database.DBRemove
import org.elastic4play.models.{ AbstractModelDef, EntityDef }

@Singleton
class DeleteSrv @Inject() (
    updateSrv: UpdateSrv,
    getSrv: GetSrv,
    dbremove: DBRemove,
    eventSrv: EventSrv,
    implicit val ec: ExecutionContext) {

  def apply[M <: AbstractModelDef[M, E], E <: EntityDef[M, E]](model: M, id: String)(implicit authContext: AuthContext): Future[E] = {
    for {
      entity ← getSrv[M, E](model, id)
      newEntity ← updateSrv.doUpdate(entity, model.removeAttribute)
      _ = eventSrv.publish(AuditOperation(newEntity, AuditableAction.Delete, JsObject.empty, authContext))
    } yield newEntity
  }

  def realDelete[M <: AbstractModelDef[M, E], E <: EntityDef[M, E]](model: M, id: String)(implicit authContext: AuthContext): Future[Unit] = {
    getSrv[M, E](model, id).flatMap(entity ⇒ realDelete(model, entity))
  }

  def realDelete[M <: AbstractModelDef[M, E], E <: EntityDef[M, E]](model: M, entity: E)(implicit authContext: AuthContext): Future[Unit] = {
    dbremove(model, entity).map { isFound ⇒
      if (isFound) eventSrv.publish(AuditOperation(entity, AuditableAction.Delete, JsObject.empty, authContext))
      else throw NotFoundError(s"${model.name} ${entity.id} not found")
    }
  }
}