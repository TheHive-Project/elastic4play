package org.elastic4play.database

import com.sksamuel.elastic4s.RefreshPolicy
import com.sksamuel.elastic4s.http.ElasticDsl._
import javax.inject.{Inject, Singleton}
import org.elastic4play.models.BaseEntity
import play.api.Logger

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

@Singleton
class DBRemove @Inject()(db: DBConfiguration, implicit val ec: ExecutionContext) {

  lazy val logger = Logger(getClass)

  def apply(entity: BaseEntity): Future[Boolean] = {
    logger.debug(s"Remove ${entity.model.modelName} ${entity.id}")
    db.execute {
        delete(entity.id)
          .from(db.indexName / "doc")
          .routing(entity.routing)
          .refresh(RefreshPolicy.WAIT_UNTIL)
      }
      .transform(r ⇒ Success(r.isSuccess))
  }
}
