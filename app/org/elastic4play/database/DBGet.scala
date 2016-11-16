package org.elastic4play.database

import javax.inject.{ Inject, Singleton }

import scala.concurrent.{ ExecutionContext, Future }

import play.api.libs.json.JsObject

import com.sksamuel.elastic4s.ElasticDsl.{ idsQuery, search }
import com.sksamuel.elastic4s.IndexesAndTypes.apply

import org.elastic4play.{ NotFoundError, Timed }
import org.elastic4play.models.Attribute

@Singleton
class DBGet @Inject() (
  db: DBConfiguration,
    implicit val ec: ExecutionContext
) {

  /**
   * Retrieve entities from ElasticSearch
   * @param modelName the name of the model (ie. document type)
   * @param id identifier of the entity to retrieve
   * @param fields optional field list to retrieve. By default all fields are retrieved. Fields "_routing", "_parent" and "_id" is automatically added.
   * @return the entity
   */

  def apply(modelName: String, id: String, fields: Option[Seq[Attribute[_]]] = None): Future[JsObject] = {
    val fieldsName = fields.fold(Seq("_source", "_routing", "_parent"))(f ⇒ "_routing" +: "_parent" +: f.map(_.name))
    db
      .execute {
        // Search by id is not possible on child entity without routing information => id query
        search in db.indexName query { idsQuery(id).types(modelName) } fields (fieldsName: _*)
      }
      .map { searchResponse ⇒
        searchResponse
          .hits
          .headOption
          .fold[JsObject](throw NotFoundError(s"$modelName $id not found")) { hit ⇒
            DBUtils.hit2json(fields, hit)
          }
      }
  }
}