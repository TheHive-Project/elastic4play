package org.elastic4play.database

import scala.concurrent.ExecutionContext.Implicits.{ global ⇒ ec }
import scala.concurrent.Future
import scala.concurrent.duration._

import play.api.Application
import play.api.inject.guice.GuiceApplicationBuilder
import play.api.libs.json.{ JsNumber, JsString, Json }
import play.api.test.PlaySpecification

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.testkit.scaladsl.TestSink
import com.sksamuel.elastic4s.searches._
import common.{ Fabricator ⇒ F }
import org.elasticsearch.search.SearchHitField
import org.junit.runner.RunWith
import org.specs2.mock.Mockito
import org.specs2.runner.JUnitRunner

import org.elastic4play.utils._

@RunWith(classOf[JUnitRunner])
class DBFindSpec extends PlaySpecification with Mockito {

  lazy val app: Application = new GuiceApplicationBuilder().build()
  implicit lazy val mat: Materializer = app.materializer
  implicit lazy val as: ActorSystem = app.actorSystem

  class DBFindWrapper {

  }
  //
  //  case class DBFindStub() {

  val pageSize = 5
  val keepAlive: FiniteDuration = 1.minute

  "DBFind" should {
    "if range is not provided, use offset:0 and limit:10" in {
      val db = mock[DBConfiguration]
      val dbfind = new DBFind(pageSize, keepAlive, db, ec, mat)
      dbfind.getOffsetAndLimitFromRange(None) must_== ((0, 10))
    }

    "if range is 75, use offset:75 and limit:10" in {
      val db = mock[DBConfiguration]
      val dbfind = new DBFind(pageSize, keepAlive, db, ec, mat)
      dbfind.getOffsetAndLimitFromRange(Some("75")) must_== ((75, 10))
    }

    "if range is 75-NaN, use it as offset:75 and limit:10" in {
      val db = mock[DBConfiguration]
      val dbfind = new DBFind(pageSize, keepAlive, db, ec, mat)
      dbfind.getOffsetAndLimitFromRange(Some("75-NaN")) must_== ((75, 10))
    }

    "if range is NaN, use it as offset:0 and limit:10" in {
      val db = mock[DBConfiguration]
      val dbfind = new DBFind(pageSize, keepAlive, db, ec, mat)
      dbfind.getOffsetAndLimitFromRange(Some("NaN")) must_== ((0, 10))
    }

    "if range is 75-32, use it as offset:75 and limit:10" in {
      val db = mock[DBConfiguration]
      val dbfind = new DBFind(pageSize, keepAlive, db, ec, mat)
      dbfind.getOffsetAndLimitFromRange(Some("75-32")) must_== ((75, 10))
    }

    "if range is 75-100, use it as offset:75 and limit:25" in {
      val db = mock[DBConfiguration]
      val dbfind = new DBFind(pageSize, keepAlive, db, ec, mat)
      dbfind.getOffsetAndLimitFromRange(Some("75-100")) must_== ((75, 25))
    }

    "if range is all, use it as offset:0 and limit:Int.MaxValue" in {
      val db = mock[DBConfiguration]
      val dbfind = new DBFind(pageSize, keepAlive, db, ec, mat)
      dbfind.getOffsetAndLimitFromRange(Some("all")) must_== ((0, Int.MaxValue))
    }

    "execute search using scroll" in {
      val db = mock[DBConfiguration]
      val dbfind = new DBFind(pageSize, keepAlive, db, ec, mat)
      val searchDef = mock[SearchDefinition]
      searchDef.limit(pageSize) returns searchDef
      searchDef.scroll(dbfind.keepAliveStr) returns searchDef
      val firstPageResult = mock[RichSearchResponse]
      val scrollId = F.string("scrollId")
      val hits = Range(0, 24)
        .map { i ⇒
          val m = mock[RichSearchHit]
          m.toString returns s"MockResult-$i"
          m
        }
        .toArray
      firstPageResult.scrollIdOpt returns Some(scrollId)
      firstPageResult.totalHits returns hits.length.toLong
      firstPageResult.isTimedOut returns false
      firstPageResult.isEmpty returns false
      firstPageResult.hits returns hits.take(5)
      db.execute(searchDef) returns Future.successful(firstPageResult)

      val secondPageResult = mock[RichSearchResponse]
      secondPageResult.scrollIdOpt returns Some(scrollId)
      secondPageResult.isTimedOut returns false
      secondPageResult.isEmpty returns false
      secondPageResult.hits returns hits.drop(5)
      db.execute(any[SearchScrollDefinition]) returns Future.successful(secondPageResult)

      val (src, total) = dbfind.searchWithScroll(searchDef, 8, 10)
      src
        .runWith(TestSink.probe[RichSearchHit])
        .request(2)
        .expectNextN(hits.slice(8, 10).toList)
        .request(5)
        .expectNextN(hits.slice(10, 13).toList)
        .request(10)
        .expectNextN(hits.slice(13, 18).toList)
        .expectComplete

      total.await must_== hits.length
      there was one(db).execute(searchDef)
      there was one(db).execute(any[SearchScrollDefinition])
      // FIXME there was one(db).execute(any[ClearScrollDefinition])
    }

    "execute search without scroll" in {
      val db = mock[DBConfiguration]
      db.indexName returns "index-test"
      val dbfind = new DBFind(pageSize, keepAlive, db, ec, mat)
      val limit = 24
      val offset = 3
      val hits = Array.fill(limit)(mock[RichSearchHit])
      val searchDef = mock[SearchDefinition]
      searchDef.limit(limit) returns searchDef
      searchDef.start(offset) returns searchDef
      val results = mock[RichSearchResponse]
      //db.execute(searchDef) returns Future.successful(results)
      doReturn(Future.successful(results)).when(db).execute(searchDef)
      results.totalHits returns 42
      results.hits returns hits

      val (src, total) = dbfind.searchWithoutScroll(searchDef, offset, limit)
      src
        .runWith(TestSink.probe[RichSearchHit])
        .request(2)
        .expectNextN(hits.take(2).toList)
        .request(10)
        .expectNextN(hits.slice(2, 12).toList)
        .request(15)
        .expectNextN(hits.drop(12).toList)
        .expectComplete

      total.await must_== 42
      there was one(db).execute(searchDef)
    }

    "convert hit to json" in {
      val hit = mock[RichSearchHit]
      val routing = F.string("routing")
      val routingField = mock[SearchHitField]
      routingField.getValue[String] returns routing
      val parent = F.string("parent")
      val parentField = mock[SearchHitField]
      parentField.getValue[String] returns parent
      val fields = Map("_routing" → RichSearchHitField(routingField), "_parent" → RichSearchHitField(parentField))
      hit.fields returns fields
      val id = F.string("id")
      hit.id returns id
      val doc = Json.obj("magic-number" → 42, "text" → "blah", "really-good" → true)
      hit.sourceAsString returns doc.toString
      val tpe = "some-object"
      hit.`type` returns tpe
      val version = 12L
      hit.version returns version

      //      val db = mock[DBConfiguration]
      //      val dbfind = new DBFind(pageSize, keepAlive, db, ec, mat)
      DBUtils.hit2json(hit) must_== (doc +
        ("_id" → JsString(id)) +
        ("_parent" → JsString(parent)) +
        ("_routing" → JsString(routing)) +
        ("_type" → JsString(tpe)) +
        ("_version" -> JsNumber(version)))
    }
  }
}