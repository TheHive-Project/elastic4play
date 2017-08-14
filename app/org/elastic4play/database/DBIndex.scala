package org.elastic4play.database

import javax.inject.{ Inject, Singleton }

import scala.concurrent.{ ExecutionContext, Future, blocking }

import play.api.Configuration

import com.sksamuel.elastic4s.ElasticDsl.{ RichFuture, index, mapping, search }
import com.sksamuel.elastic4s.IndexesAndTypes.apply

import org.elastic4play.models.{ ChildModelDef, ModelAttributes, ModelDef }

@Singleton
class DBIndex(
    db: DBConfiguration,
    nbShards: Int,
    nbReplicas: Int,
    implicit val ec: ExecutionContext) {

  @Inject() def this(
    configuration: Configuration,
    db: DBConfiguration,
    ec: ExecutionContext) = this(
    db,
    configuration.getOptional[Int]("search.nbshards").getOrElse(5),
    configuration.getOptional[Int]("search.nbreplicas").getOrElse(1),
    ec)

  /**
   * Create a new index. Collect mapping for all attributes of all entities
   *
   * @param models list of all ModelAttributes to used in order to build index mapping
   * @return a future which is completed when index creation is finished
   */
  def createIndex(models: Iterable[ModelAttributes]): Future[Unit] = {
    val modelsMapping = models
      .map {
        case model: ModelDef[_, _]            ⇒ mapping(model.name) fields model.attributes.filterNot(_.name == "_id").map(_.elasticMapping) dateDetection false numericDetection false
        case model: ChildModelDef[_, _, _, _] ⇒ mapping(model.name) fields model.attributes.filterNot(_.name == "_id").map(_.elasticMapping) parent model.parentModel.name dateDetection false numericDetection false
      }
      .toSeq
    db.execute {
      com.sksamuel.elastic4s.ElasticDsl.create
        .index(db.indexName)
        .mappings(modelsMapping: _*)
        .shards(nbShards)
        .replicas(nbReplicas)
    }
      .map { _ ⇒ () }
  }

  /**
   * Tests whether the index exists
   *
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
   *
   * @return true if the index exists
   */
  def indexStatus: Boolean = blocking {
    getIndexStatus.await
  }

  /**
   * Get the number of document of this type
   *
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