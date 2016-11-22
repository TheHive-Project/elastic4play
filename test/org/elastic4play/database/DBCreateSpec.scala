package org.elastic4play.database

import scala.concurrent.Future

import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import org.specs2.mock.Mockito

import play.api.libs.json.{ Json, JsObject, JsString }
import play.api.libs.iteratee.Execution.trampoline
import play.api.test.PlaySpecification

import org.elasticsearch.action.bulk.BulkItemResponse
import org.elasticsearch.action.index.{ IndexRequest, IndexResponse }
import com.sksamuel.elastic4s.{ IndexResult, IndexDefinition, BulkResult, BulkItemResult, BulkDefinition }

import org.elastic4play.models.BaseEntity
import common.{ Fabricator ⇒ F }
import org.elastic4play.utils._

@RunWith(classOf[JUnitRunner])
class DBCreateSpec extends PlaySpecification with Mockito {
  val modelName = F.string("modelName")
  val defaultEntityId = F.string("defaultEntityId")
  val sampleDoc = Json.obj("caseId" → 42, "title" → "Test case", "description" → "Case used for unit test", "tags" → Seq("test", "specs"))

  class DBCreateWrapper {
    val db = mock[DBConfiguration]
    val dbcreate = new DBCreate(db, trampoline)

    implicit val ec = trampoline

    def apply(modelName: String, attributes: JsObject) = {
      val indexResult = mock[IndexResult]
      indexResult.getId returns (attributes \ "_id").asOpt[String].getOrElse(defaultEntityId)
      db.execute(any[IndexDefinition]) returns Future.successful(indexResult)
      val attrs = dbcreate(modelName, attributes).await
      val captor = capture[IndexDefinition]
      there was one(db).execute(captor.capture)
      (attrs, captor.value.build)
    }

    def apply(parent: BaseEntity, attributes: JsObject) = {
      val indexResult = mock[IndexResult]
      indexResult.getId returns (attributes \ "_id").asOpt[String].getOrElse(defaultEntityId)
      db.execute(any[IndexDefinition]) returns Future.successful(indexResult)
      val attrs = dbcreate(modelName, Some(parent), attributes).await
      val captor = capture[IndexDefinition]
      there was one(db).execute(captor.capture)
      (attrs, captor.value.build)
    }
  }

  "DBCreate" should {
    "create document without id, parent or routing" in {
      val dbcreate = new DBCreateWrapper
      val (returnAttrs, indexDef) = dbcreate(modelName, sampleDoc)
      (returnAttrs \ "_type").asOpt[String] must beSome(modelName)
      (returnAttrs \ "_id").asOpt[String] must beSome(defaultEntityId)
      (returnAttrs \ "_routing").asOpt[String] must beSome(defaultEntityId)
      (returnAttrs \ "_parent").asOpt[String] must beNone
      indexDef.id() must beNull
      indexDef.parent() must beNull
      indexDef.routing() must beNull
    }

    "create document with id, parent and routing" in {
      val entityId = F.string("entityId")
      val routing = F.string("routing")
      val parentId = F.string("parentId")
      val dbcreate = new DBCreateWrapper()
      val (returnAttrs, indexDef) = dbcreate(modelName, sampleDoc +
        ("_id" → JsString(entityId)) +
        ("_routing" → JsString(routing)) +
        ("_parent" → JsString(parentId)))

      (returnAttrs \ "_type").asOpt[String] must beSome(modelName)
      (returnAttrs \ "_id").asOpt[String] must beSome(entityId)
      (returnAttrs \ "_routing").asOpt[String] must beSome(routing)
      (returnAttrs \ "_parent").asOpt[String] must beSome(parentId)
      indexDef.id() must_== entityId
      indexDef.parent() must_== parentId
      indexDef.routing() must_== routing
    }

    "create document with id and parent entity" in {
      val entityId = F.string("entityId")
      val routing = F.string("routing")
      val parentId = F.string("parentId")

      val dbcreate = new DBCreateWrapper()
      val parent = mock[BaseEntity]
      parent.id returns parentId
      parent.routing returns routing
      val (returnAttrs, indexDef) = dbcreate(parent, sampleDoc + ("_id" → JsString(entityId)))

      (returnAttrs \ "_type").asOpt[String] must beSome(modelName)
      (returnAttrs \ "_id").asOpt[String] must beSome(entityId)
      (returnAttrs \ "_routing").asOpt[String] must beSome(routing)
      (returnAttrs \ "_parent").asOpt[String] must beSome(parentId)
      indexDef.id() must_== entityId
      indexDef.parent() must_== parentId
      indexDef.routing() must_== routing
    }
  }
}