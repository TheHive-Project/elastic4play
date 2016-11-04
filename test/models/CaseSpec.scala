//package models
//
//import java.util.Date
//import javax.inject.Provider
//
//import scala.collection.JavaConversions._
//import scala.concurrent.Future
//import scala.concurrent.duration._
//import scala.reflect.ClassTag
//
//import play.api.libs.json.{ Json, JsObject, JsString }
//import play.api.cache.CacheApi
//import play.api.test.{ PlaySpecification, FakeRequest }
//
//import org.specs2.runner.JUnitRunner
//import org.junit.runner.RunWith
//import org.specs2.mock.Mockito
//
//import org.elasticsearch.action.index.IndexResponse
//import org.elasticsearch.action.get.GetResponse
//import org.elasticsearch.action.update.UpdateResponse
//import org.elasticsearch.index.get.{ GetResult, GetField }
//import org.elasticsearch.search.{ SearchHits, SearchHit }
//
//import com.sksamuel.elastic4s.ElasticDsl.RichFuture
//
//import database.{ TableDeps, Sequence, DefaultDatabase, UserAttributeApi, ModelEntity }
//import common.{ Model, FakeCache, AuthContext, Fields, NotFoundError, StringInputValue, JsonInputValue }
//
//case class Provide[T](underlying: T) extends Provider[T] {
//  override def get() = underlying
//}
//
//@RunWith(classOf[JUnitRunner])
//class CaseSpec extends PlaySpecification with Mockito {
//
//  case class Context() {
//    val db = mock[DefaultDatabase] //.verbose
//    val userAttributeService = mock[UserAttributeApi]
//    userAttributeService.get("case") returns Future.successful(Nil)
//    val dataStore = mock[DataStore]
//    val audits = mock[Audits]
//    audits.createAudit(anyString, any[ModelEntity], any[AuditOperation.Type], any[JsObject])(any[AuthContext]) returns Future.successful(mock[audits.Audit])
//    val dblists = mock[DBLists]
//    val sequence = mock[Sequence]
//    val cache = FakeCache
//    val cases = spy(new Cases(sequence, TableDeps(db, userAttributeService, dataStore, cache, Provide(audits), dblists)))
//    val users = Model[Users]
//    val authContext = new AuthContext {
//      override val user = users.getEntity("init", None, Json.obj("login" -> "init", "roles" -> List("ADMIN", "WRITE", "READ")))
//    }
//  }
//
//  "Case" should {
//    "find entity using its id" in {
//      val ctx = Context()
//      val caseJson = Json.obj(
//        "caseId" -> 42,
//        "title" -> "case for unit tests",
//        "description" -> "this case is use for unit tests",
//        "severity" -> 0,
//        "startDate" -> new Date,
//        "status" -> CaseStatus.Open)
//      val caseId = "get_existant_id"
//      val getResult = spy(new GetResult("test_index", "case", caseId, 1, true, null, Map.empty[String, GetField]))
//      getResult.sourceAsString returns caseJson.toString
//      ctx.db.getEntity("case", caseId) returns Future.successful(new GetResponse(getResult))
//
//      val getCase = ctx.cases.apply(caseId).await
//
//      getCase must be equalTo new ctx.cases.Case(caseId, caseJson)
//    }
//
//    "throw a exception if entity is not found" in {
//      val ctx = Context()
//      val caseId = "get_non_existant_id"
//      val getResult = new GetResult("test_index", "case", null, 0, false, null, Map.empty[String, GetField])
//      ctx.db.getEntity("case", caseId) returns Future.successful(new GetResponse(getResult))
//
//      ctx.cases.apply(caseId).await must throwAn[NotFoundError]
//    }
//
//    "be able to create entity using attribute map" in {
//      val ctx = Context()
//      val caseId = "newCaseId"
//      ctx.sequence.getNext("case") returns Future.successful(42)
//      val inputAttributes = Fields.empty +
//        ("title", "we're under attack") +
//        ("description", "description") +
//        ("startDate", "20150627T193509+0200")
//      val caseAttributes = Json.obj(
//        "title" -> "we're under attack",
//        "description" -> "description",
//        "startDate" -> "20150627T193509+0200",
//        "status" -> CaseStatus.Open,
//        "caseId" -> 42,
//        "flag" -> false,
//        "tlp" -> -1,
//        "user" -> ctx.authContext.user.id)
//      val indexResponse = mock[IndexResponse]
//      indexResponse.getId returns caseId
//      ctx.db.create("case", caseAttributes) returns Future.successful(indexResponse)
//
//      val aCase = ctx.cases.create(inputAttributes)(ctx.authContext).await
//
//      aCase must be equalTo ctx.cases.Case(caseId, caseAttributes)
//      there was one(ctx.db).create("case", caseAttributes)
//      there was one(ctx.audits).createAudit("case", aCase, AuditOperation.Creation, caseAttributes)(ctx.authContext)
//    }
//
//    "find entity using filter" in {
//      val ctx = Context()
//      val filter = Seq("user" -> "init", "title" -> "Title")
//      val range = Some("0-100")
//      val sortBy = Seq("-startDate")
//      val totalHits = 123
//      val cases = Map(
//        "first_id" -> Json.obj("caseId" -> "101", "title" -> "Title of case 101", "description" -> "description of case 101", "startDate" -> new Date, "flag" -> true, "status" -> CaseStatus.Open),
//        "second_id" -> Json.obj("caseId" -> "102", "title" -> "Title of case 102", "description" -> "description of case 102", "startDate" -> new Date, "flag" -> true, "status" -> CaseStatus.Resolved),
//        "last_id" -> Json.obj("caseId" -> "103", "title" -> "Title of case 103", "description" -> "description of case 103", "startDate" -> new Date, "flag" -> false, "status" -> CaseStatus.Ephemeral))
//      val hitsIterator = cases.toIterator.map {
//        case (i, a) =>
//          val hit = mock[SearchHit]
//          hit.getId returns i
//          hit.getSourceAsString returns a.toString()
//          hit
//      }
//      ctx.db.find(Some("case"), filter, range, sortBy) returns Future.successful((totalHits, hitsIterator))
//
//      val (caseCount, foundCases) = ctx.cases.findFilter(filter, range, Nil).await
//
//      foundCases.toSeq must be equalTo cases.map { case (i, a) => ctx.cases.read(i, None, a) }.toSeq
//      caseCount must be equalTo totalHits
//    }
//
//    "find entity using query" in {
//      val ctx = Context()
//      val query = "user:init AND title:Title"
//      val range = Some("0-100")
//      val sortBy = Seq("-startDate")
//      val totalHits = 123
//      val cases = Map(
//        "first_id" -> Json.obj("caseId" -> "101", "title" -> "Title of case 101", "description" -> "description of case 101", "startDate" -> new Date, "flag" -> true, "status" -> CaseStatus.Open),
//        "second_id" -> Json.obj("caseId" -> "102", "title" -> "Title of case 102", "description" -> "description of case 102", "startDate" -> new Date, "flag" -> true, "status" -> CaseStatus.Resolved),
//        "last_id" -> Json.obj("caseId" -> "103", "title" -> "Title of case 103", "description" -> "description of case 103", "startDate" -> new Date, "flag" -> false, "status" -> CaseStatus.Ephemeral))
//      val hitsIterator = cases.toIterator.map {
//        case (i, a) =>
//          val hit = mock[SearchHit]
//          hit.getId returns i
//          hit.getSourceAsString returns a.toString()
//          hit
//      }
//      ctx.db.find(Some("case"), query, range, sortBy) returns Future.successful((totalHits, hitsIterator))
//
//      val (caseCount, foundCases) = ctx.cases.findQuery(query, range, Nil).await
//
//      foundCases.toSeq must be equalTo cases.map { case (i, a) => ctx.cases.read(i, None, a) }.toSeq
//      caseCount must be equalTo totalHits
//    }
//
//    "update entity attribute using json" in {
//      val ctx = Context()
//      val caseJson = Json.obj(
//        "caseId" -> 42,
//        "title" -> "case for unit tests",
//        "description" -> "this case is use for unit tests",
//        "severity" -> 0,
//        "startDate" -> new Date,
//        "flag" -> true,
//        "status" -> CaseStatus.Open,
//        "user" -> "an_user_id")
//      val updateAttributes = Json.obj("title" -> "another title")
//      val caseId = "get_existant_id"
//      val aCase = ctx.cases.read(caseId, None, caseJson)
//      val getResult = mock[GetResult]
//      getResult.isExists() returns true
//      getResult.sourceAsString() returns (caseJson ++ updateAttributes).toString
//      val updateResponse = mock[UpdateResponse]
//      updateResponse.getId returns caseId
//      updateResponse.getGetResult returns getResult
//      ctx.db.modify("case", aCase.id, aCase.id, updateAttributes) returns Future.successful(updateResponse)
//
//      val newCase = ctx.cases.modify(aCase, updateAttributes)(ctx.authContext).await
//
//      newCase must be equalTo ctx.cases.read(caseId, None, caseJson ++ updateAttributes)
//      there was one(ctx.audits).createAudit("case", aCase, AuditOperation.Update, updateAttributes)(ctx.authContext)
//    }
//
//    "update entity attribute using attribute map" in {
//      val ctx = Context()
//      val caseJson = Json.obj(
//        "caseId" -> 42,
//        "title" -> "case for unit tests",
//        "description" -> "this case is use for unit tests",
//        "severity" -> 0,
//        "startDate" -> new Date,
//        "flag" -> true,
//        "status" -> CaseStatus.Open,
//        "user" -> "an_user_id")
//      val updateAttributesMap = Fields.empty + ("title", "another title")
//      val updateAttributesJson = Json.obj("title" -> "another title")
//      val caseId = "get_existant_id"
//      val aCase = ctx.cases.read(caseId, None, caseJson)
//      val getResult = mock[GetResult]
//      getResult.isExists() returns true
//      getResult.sourceAsString() returns (caseJson ++ updateAttributesJson).toString
//      val updateResponse = mock[UpdateResponse]
//      updateResponse.getId returns caseId
//      updateResponse.getGetResult returns getResult
//      ctx.db.modify("case", aCase.id, aCase.id, updateAttributesJson) returns Future.successful(updateResponse)
//
//      val newCase = ctx.cases.modify(aCase, updateAttributesMap)(ctx.authContext).await
//
//      newCase must be equalTo ctx.cases.read(caseId, None, caseJson ++ updateAttributesJson)
//      there was one(ctx.audits).createAudit("case", aCase, AuditOperation.Update, updateAttributesJson)(ctx.authContext)
//    }
//
//    "remove entity" in {
//      val ctx = Context()
//      val caseId = "removed_case_id"
//      val aCase = spy(ctx.cases.read(caseId, None, JsObject(Nil)))
//      aCase.id returns caseId
//      aCase.routing returns caseId
//      doReturn(Future.successful(aCase)).when(ctx.cases).apply(caseId)
//      ctx.db.modify("case", caseId, caseId, """ctx._source["status"]=attr""", Map("attr" -> "Deleted")) returns Future.successful(mock[UpdateResponse])
//
//      val result = ctx.cases.remove(caseId)(ctx.authContext).await
//
//      result must be equalTo (())
//      there was one(ctx.audits).createAudit("case", aCase, AuditOperation.Delete, JsObject(Nil))(ctx.authContext)
//    }
//  }
//}