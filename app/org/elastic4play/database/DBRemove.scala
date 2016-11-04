package org.elastic4play.database

import javax.inject.{ Inject, Singleton }

import scala.concurrent.{ ExecutionContext, Future }

import org.elastic4play.Timed
import org.elastic4play.models.{ BaseEntity, BaseModelDef }

import com.sksamuel.elastic4s.ElasticDsl.delete
import com.sksamuel.elastic4s.IndexAndTypes.apply

@Singleton
class DBRemove @Inject() (db: DBConfiguration,
                          implicit val ec: ExecutionContext) {

  def apply(model: BaseModelDef, entity: BaseEntity): Future[Boolean] = {
    db.execute {
      delete id entity.id from db.indexName -> model.name routing entity.routing refresh true
    }
      .map { deleteResponse =>
        deleteResponse.isFound()
      }
  }
}