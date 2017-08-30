package org.elastic4play.database

import javax.inject.{ Inject, Singleton }

import scala.concurrent.{ ExecutionContext, Future }

import com.sksamuel.elastic4s.ElasticDsl.{ RichString, delete }
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy
import org.elasticsearch.rest.RestStatus

import org.elastic4play.models.{ BaseEntity, BaseModelDef }

@Singleton
class DBRemove @Inject() (
    db: DBConfiguration,
    implicit val ec: ExecutionContext) {

  def apply(model: BaseModelDef, entity: BaseEntity): Future[Boolean] = {
    db.execute {
      delete(entity.id)
        .from(db.indexName / model.name)
        .routing(entity.routing)
        .refresh(RefreshPolicy.IMMEDIATE)
    }
      .map { deleteResponse ⇒
        deleteResponse.status != RestStatus.NOT_FOUND
      }
  }
}