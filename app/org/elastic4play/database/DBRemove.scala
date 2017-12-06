package org.elastic4play.database

import javax.inject.{ Inject, Singleton }

import scala.concurrent.{ ExecutionContext, Future }

import com.sksamuel.elastic4s.http.ElasticDsl.deleteById
import com.sksamuel.elastic4s.RefreshPolicy

import org.elastic4play.models.{ BaseEntity, BaseModelDef }

@Singleton
class DBRemove @Inject() (
    db: DBConfiguration,
    implicit val ec: ExecutionContext) {

  def apply(model: BaseModelDef, entity: BaseEntity): Future[Boolean] = {
    db.execute {
      deleteById(db.indexName, model.modelName, entity.id)
        .routing(entity.routing)
        .refresh(RefreshPolicy.WAIT_UNTIL)
    }
      .map(_.result == "deleted")
  }
}