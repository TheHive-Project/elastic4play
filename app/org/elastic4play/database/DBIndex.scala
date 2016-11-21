package org.elastic4play.database

import javax.inject.{ Inject, Singleton }

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.blocking

import com.sksamuel.elastic4s.ElasticDsl.{ RichFuture, index, mapping, search }
import com.sksamuel.elastic4s.IndexesAndTypes.apply

import org.elastic4play.Timed
import org.elastic4play.models.{ ChildModelDef, ModelAttributes, ModelDef }

@Singleton
class DBIndex @Inject() (
    db: DBConfiguration,
    implicit val ec: ExecutionContext) {

  /**
   * Create a new index. Collect mapping for all attributes of all entities
   * @param models list of all ModelAttributes to used in order to build index mapping
   * @return a future which is completed when index creation is finished
   */
  def createIndex(models: Iterable[ModelAttributes]) = {
    val modelsMapping = models
      .map {
        case model: ModelDef[_, _]            ⇒ mapping(model.name) fields model.attributes.filterNot(_.name == "_id").map(_.elasticMapping) dateDetection false numericDetection false
        case model: ChildModelDef[_, _, _, _] ⇒ mapping(model.name) fields model.attributes.filterNot(_.name == "_id").map(_.elasticMapping) parent model.parentModel.name dateDetection false numericDetection false
      }
      .toSeq
    db.execute {
      com.sksamuel.elastic4s.ElasticDsl.create index db.indexName mappings (modelsMapping: _*)
    }
      .map { _ ⇒ () }
  }

  /**
   * Tests whether the index exists
   * @return future of true if the index exists
   */
  def getIndexStatus: Future[Boolean] = {
    db.execute {
      index exists db.indexName
    } map { indicesExistsResponse ⇒
      indicesExistsResponse.isExists
    }
  }

  /**
   * Tests whether the index exists
   * @return true if the index exists
   */
  def indexStatus: Boolean = blocking {
    getIndexStatus.await
  }

  /**
   * Get the number of document of this type
   * @param modelName name of the document type from which the count must be done
   * @return document count
   */
  def getSize(modelName: String): Future[Long] = db.execute {
    search in db.indexName → modelName size 0
  } map { searchResponse ⇒
    searchResponse.totalHits
  } recover {
    case _ ⇒ 0L
  }
}