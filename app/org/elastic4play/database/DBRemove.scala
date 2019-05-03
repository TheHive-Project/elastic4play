package org.elastic4play.database

import javax.inject.{Inject, Singleton}

import scala.concurrent.{ExecutionContext, Future}

import play.api.Logger

import com.sksamuel.elastic4s.ElasticDsl.{delete, RichString}
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy
import org.elasticsearch.rest.RestStatus

import org.elastic4play.models.BaseEntity

@Singleton
class DBRemove @Inject()(db: DBConfiguration, implicit val ec: ExecutionContext) {

  lazy val logger = Logger(getClass)

  def apply(entity: BaseEntity): Future[Boolean] = {
    logger.debug(s"Remove ${entity.model.modelName} ${entity.id}")
    db.execute {
        delete(entity.id)
          .from(db.indexName / entity.model.modelName)
          .routing(entity.routing)
          .refresh(RefreshPolicy.WAIT_UNTIL)
      }
      .map { deleteResponse ⇒
        deleteResponse.status != RestStatus.NOT_FOUND
      }
  }
}
