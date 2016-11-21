package org.elastic4play.database

import javax.inject.{ Inject, Singleton }

import scala.concurrent.{ ExecutionContext, Future }
import scala.language.postfixOps
import scala.util.{ Failure, Success, Try }

import play.api.Logger
import play.api.libs.json.{ JsArray, JsBoolean, JsNull, JsNumber, JsObject, JsString, JsValue, Json }

import org.elastic4play.{ Timed, UpdateError }
import org.elastic4play.models.BaseEntity
import org.elasticsearch.action.update.UpdateResponse

import com.sksamuel.elastic4s.ElasticDsl.{ bulk, script, update }
import com.sksamuel.elastic4s.IndexAndTypes.apply

@Singleton
class DBModify @Inject() (
    db: DBConfiguration,
    implicit val ec: ExecutionContext) {
  val log = Logger(getClass)

  /**
   * Convert JSON value to java native value
   */
  private[database] def jsonToAny(json: JsValue): Any = {
    import scala.collection.JavaConversions._
    json match {
      case v: JsObject  ⇒ mapAsJavaMap(v.fields.toMap.mapValues(jsonToAny))
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
   * @return update parameters needed for execution
   */
  private[database] def buildScript(entity: BaseEntity, updateAttributes: JsObject): UpdateParams = {
    import scala.collection.JavaConversions._

    val attrs = updateAttributes.fields.zipWithIndex
    val updateScript = attrs.map {
      case ((name, JsArray(Nil)), index) ⇒
        val names = name.split("\\.")
        names.init.map(n ⇒ s"""["$n"]""").mkString("ctx._source", "", s""".remove("${names.last}")""")
      case ((name, JsNull), index) ⇒
        name.split("\\.").map(n ⇒ s"""["$n"]""").mkString("ctx._source", "", s"=null")
      case ((name, _), index) ⇒
        name.split("\\.").map(n ⇒ s"""["$n"]""").mkString("ctx._source", "", s"=param$index")
    } mkString (";")

    val parameters = jsonToAny(JsObject(attrs.collect {
      case ((name, value), index) if value != JsArray(Nil) && value != JsNull ⇒ s"param$index" → value
    })).asInstanceOf[java.util.Map[String, Any]]

    UpdateParams(entity, updateScript, parameters.toMap, updateAttributes)
  }

  private[database] case class UpdateParams(entity: BaseEntity, updateScript: String, params: Map[String, Any], attributes: JsObject) {
    def updateDef = update id entity.id in s"${db.indexName}/${entity.model.name}" routing entity.routing script { script(updateScript).params(params) } fields "_source" retryOnConflict 5
    def result(attrs: JsObject) =
      entity.model(attrs +
        ("_type" → JsString(entity.model.name)) +
        ("_id" → JsString(entity.id)) +
        ("_routing" → JsString(entity.routing)) +
        ("_parent" → entity.parentId.fold[JsValue](JsNull)(JsString)))
  }

  /**
   * Update entity with new attributes contained in JSON object
   * @param entity entity to update
   * @param updateAttributes contains attributes to update. JSON object contains key (attribute name) and value.
   *   Sub attribute can be updated using dot notation ("attr.subattribute").
   * @return new version of the entity
   */
  def apply(entity: BaseEntity, updateAttributes: JsObject): Future[BaseEntity] = {
    executeScript(buildScript(entity, updateAttributes))
  }

  private[database] def executeScript(params: UpdateParams): Future[BaseEntity] = {
    db.execute(params.updateDef refresh true)
      .map { updateResponse ⇒
        params.result(Json.parse(updateResponse.getGetResult.sourceAsString).as[JsObject])
      }
  }

  private[database] def executeScript(params: Seq[UpdateParams]): Future[Seq[Try[BaseEntity]]] = {
    if (params.isEmpty)
      return Future.successful(Nil)
    db.execute(bulk(params.map(_.updateDef)) refresh true)
      .map { bulkResponse ⇒
        bulkResponse.items.zip(params).map {
          case (bulkItemResponse, p) if bulkItemResponse.isFailure ⇒
            val failure = bulkItemResponse.failure
            log.warn(s"create failure : ${failure.getIndex} ${failure.getId} ${failure.getType} ${failure.getMessage} ${failure.getStatus}")
            Failure(UpdateError(Option(failure.getStatus.name), failure.getMessage, p.attributes))
          case (bulkItemResponse, p) ⇒
            val updateResponse = bulkItemResponse.original.getResponse[UpdateResponse]
            Success(p.result(Json.parse(updateResponse.getGetResult.sourceAsString).as[JsObject]))
        }
      }
  }
}