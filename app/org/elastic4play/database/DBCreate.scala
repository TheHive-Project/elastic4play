package org.elastic4play.database

import javax.inject.{ Inject, Singleton }

import scala.concurrent.{ ExecutionContext, Future }

import play.api.Logger
import play.api.libs.json.JsValue.jsValueToJsLookup
import play.api.libs.json._

import akka.stream.scaladsl.Sink
import com.sksamuel.elastic4s.RefreshPolicy
import com.sksamuel.elastic4s.http.ElasticDsl.indexInto
import com.sksamuel.elastic4s.http.ElasticError
import com.sksamuel.elastic4s.indexes.IndexDefinition
import com.sksamuel.elastic4s.streams.RequestBuilder

import org.elastic4play.models.BaseEntity
import org.elastic4play.{ InternalError, SearchError }

/**
  * Service lass responsible for entity creation
  * This service doesn't check any attribute conformity (according to model)
  */
@Singleton
class DBCreate @Inject() (
    db: DBConfiguration,
    implicit val ec: ExecutionContext) {

  private[DBCreate] lazy val logger = Logger(getClass)

  /**
    * Create an entity of type "modelName" with attributes
    *
    * @param modelName name of the model of the creating entity
    * @param attributes JSON object containing attributes of the creating entity. Attributes can contain _id, _parent and _routing.
    * @return created entity attributes with _id and _routing (and _parent if entity is a child)
    */
  def apply(modelName: String, attributes: JsObject): Future[JsObject] = {
    apply(modelName, None, attributes)
  }

  /**
    * Create an entity of type modelName with attributes and optionally a parent
    *
    * @param modelName name of the model of the creating entity
    * @param parent parent of the creating entity (if model is ChildModelDef
    * @param attributes JSON object containing attributes of the creating entity.
    * Attributes can contain _id, _parent and _routing. Routing and parent informations are extracted from parent parameter (if present)
    * @return created entity attributes with _id and _routing (and _parent if entity is a child)
    */
  def apply(modelName: String, parent: Option[BaseEntity], attributes: JsObject): Future[JsObject] = {
    val id = (attributes \ "_id").asOpt[String]
    val parentId = parent.map(_.id)
      .orElse((attributes \ "_parent").asOpt[String])
    val routing = parent.map(_.routing)
      .orElse((attributes \ "_routing").asOpt[String])
      .orElse(id)

    // remove attributes that starts with "_" because we wan't permit to interfere with elasticsearch internal fields
    val docSource = JsObject(attributes.fields.filterNot(_._1.startsWith("_"))).toString
    db
      .execute {
        addId(id).andThen(addParent(parentId)).andThen(addRouting(routing)) {
          indexInto(db.indexName, modelName).source(docSource).refresh(RefreshPolicy.WAIT_UNTIL)
        }
      }
      .map { indexResponse ⇒
        attributes +
          ("_type" → JsString(modelName)) +
          ("_id" → JsString(indexResponse.id)) +
          ("_parent" → parentId.fold[JsValue](JsNull)(JsString)) +
          ("_version" -> JsNumber(indexResponse.version)) +
          ("_routing" → JsString(routing.getOrElse(indexResponse.id)))
      }
      .recoverWith {
        case SearchError(_, _, error) ⇒ Future.failed(convertError(attributes, error))
      }
  }

  private[database] def convertError(attributes: JsObject, error: ElasticError): Throwable = error match {
    //    case ElasticError(???rte: RemoteTransportException        ⇒ convertError(attributes, rte.getCause)
    //    case vcee: VersionConflictEngineException ⇒ ConflictError(vcee.getMessage, attributes)
    //    case other ⇒
    //      logger.warn("create error", other)
    //      CreateError(None, other.getMessage, attributes)
    // FIXME
    case _ ⇒ ???
  }

  /**
    * add id information in index definition
    */
  private def addId(id: Option[String]): IndexDefinition ⇒ IndexDefinition = id match {
    case Some(i) ⇒ _ id i createOnly true
    case None    ⇒ identity
  }
  /**
    * add parent information in index definition
    */
  private def addParent(parent: Option[String]): IndexDefinition ⇒ IndexDefinition = parent match {
    case Some(p) ⇒ _ parent p
    case None    ⇒ identity
  }

  /**
    * add routing information in index definition
    */
  private def addRouting(routing: Option[String]): IndexDefinition ⇒ IndexDefinition = routing match {
    case Some(r) ⇒ _ routing r
    case None    ⇒ identity
  }

  /**
    * Class used to build index definition based on model name and attributes
    * This class is used by sink (ElasticSearch reactive stream)
    */
  private class AttributeRequestBuilder() extends RequestBuilder[JsObject] {
    override def request(attributes: JsObject): IndexDefinition = {
      val docSource = JsObject(attributes.fields.filterNot(_._1.startsWith("_"))).toString
      val id = (attributes \ "_id").asOpt[String]
      val parent = (attributes \ "_parent").asOpt[String]
      val routing = (attributes \ "_routing").asOpt[String] orElse parent orElse id
      val modelName = (attributes \ "_type").asOpt[String].getOrElse(throw InternalError("The entity doesn't contain _type attribute"))
      addId(id).andThen(addParent(parent)).andThen(addRouting(routing)) {
        indexInto(db.indexName, modelName).source(docSource)
      }
    }
  }

  /**
    * build a akka stream sink that create entities
    */
  def sink(): Sink[JsObject, Future[Unit]] = db.sink(new AttributeRequestBuilder())
}