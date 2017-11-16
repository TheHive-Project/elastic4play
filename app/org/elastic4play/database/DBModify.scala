package org.elastic4play.database

import javax.inject.{ Inject, Singleton }

import scala.concurrent.{ ExecutionContext, Future }

import play.api.Logger
import play.api.libs.json._

import com.sksamuel.elastic4s.ElasticDsl.{ script, update }
import com.sksamuel.elastic4s.script.ScriptDefinition
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy

import org.elastic4play.models.BaseEntity

@Singleton
class DBModify @Inject() (
    db: DBConfiguration,
    implicit val ec: ExecutionContext) {
  private[DBModify] lazy val logger = Logger(getClass)

  /**
    * Convert JSON value to java native value
    */
  private[database] def jsonToAny(json: JsValue): Any = {
    import scala.collection.JavaConverters._
    json match {
      case v: JsObject  ⇒ v.fields.toMap.mapValues(jsonToAny).asJava
      case v: JsArray   ⇒ v.value.map(jsonToAny).toArray
      case v: JsNumber  ⇒ v.value.toLong
      case v: JsString  ⇒ v.value
      case v: JsBoolean ⇒ v.value
      case JsNull       ⇒ null
    }
  }

  /**
    * Build the parameters needed to update ElasticSearch document
    * Parameters contains update script, parameters for the script
    * As null is a valid value to set, in order to remove an attribute an empty array must be used.
    * @param entity entity to update
    * @param updateAttributes contains attributes to update. JSON object contains key (attribute name) and value.
    *   Sub attribute can be updated using dot notation ("attr.subattribute").
    * @return ElasticSearch update script
    */
  private[database] def buildScript(entity: BaseEntity, updateAttributes: JsObject): ScriptDefinition = {
    import scala.collection.JavaConverters._

    val attrs = updateAttributes.fields.zipWithIndex
    val updateScript = attrs.map {
      case ((name, JsArray(Seq())), _) ⇒
        val names = name.split("\\.")
        names.init.map(n ⇒ s"""["$n"]""").mkString("ctx._source", "", s""".remove("${names.last}")""")
      case ((name, JsNull), _) ⇒
        name.split("\\.").map(n ⇒ s"""["$n"]""").mkString("ctx._source", "", s"=null")
      case ((name, _), index) ⇒
        name.split("\\.").map(n ⇒ s"""["$n"]""").mkString("ctx._source", "", s"=params.param$index")
    } mkString ";"

    val parameters = jsonToAny(JsObject(attrs.collect {
      case ((_, value), index) if value != JsArray(Nil) && value != JsNull ⇒ s"param$index" → value
    })).asInstanceOf[java.util.Map[String, Any]].asScala.toMap

    script(updateScript).params(parameters)
  }

  /**
    * Update entity with new attributes contained in JSON object
    * @param entity entity to update
    * @param updateAttributes contains attributes to update. JSON object contains key (attribute name) and value.
    *   Sub attribute can be updated using dot notation ("attr.subattribute").
    * @return new version of the entity
    */
  def apply(entity: BaseEntity, updateAttributes: JsObject): Future[BaseEntity] = {
    db
      .execute {
        update(entity.id)
          .in(db.indexName → entity.model.modelName)
          .routing(entity.routing)
          .script(buildScript(entity, updateAttributes))
          .fetchSource(true)
          .retryOnConflict(5)
          .refresh(RefreshPolicy.WAIT_UNTIL)
      }
      .map { updateResponse ⇒
        entity.model(Json.parse(updateResponse.get.sourceAsString).as[JsObject] +
          ("_type" → JsString(entity.model.modelName)) +
          ("_id" → JsString(entity.id)) +
          ("_routing" → JsString(entity.routing)) +
          ("_parent" → entity.parentId.fold[JsValue](JsNull)(JsString)))
      }
  }
}