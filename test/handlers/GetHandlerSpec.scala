//package handlers
//
//import java.util.Date
//
//import scala.concurrent.Future
//
//import play.api.mvc.{ RequestHeader, Result }
//import play.api.Configuration
//import play.api.libs.json.Json
//import play.api.test.{ PlaySpecification, FakeRequest }
//
//import org.specs2.runner.JUnitRunner
//import org.junit.runner.RunWith
//import org.specs2.mock.Mockito
//
//import com.sksamuel.elastic4s.ElasticDsl.RichFuture
//
//import models.{ Users, Cases, CaseStatus }
//import database.{ BaseTable }
//import common.{ Model, NotTimed, NotFoundError, AuthorizationError }
//
//@RunWith(classOf[JUnitRunner])
//class GetHandlerSpec extends PlaySpecification with Mockito {
//
//  val users = Model[Users]
//  doReturn(Future.successful(0L)).when(users.table).getSize
//  val initUser = users.getEntity("init", None, Json.obj("login" -> "init", "roles" -> List("ADMIN", "WRITE", "READ")))
//  users.table.getInitialUser returns Future.successful(initUser)
//
//  val cases = Model[Cases]
//  val aCase = cases.getEntity("a_case_id", None, Json.obj(
//    "caseId" -> 42,
//    "title" -> "case for unit tests",
//    "description" -> "this case is use for unit tests",
//    "severity" -> 0,
//    "startDate" -> new Date,
//    "status" -> CaseStatus.Open))
//
//  def getHandler[T <: BaseTable](model: Model[T], entityId: String, role: String = "READ") = {
//    val h = new GetHandler(users.table, Configuration.empty, NotTimed)
//    val h2 = new h.Get(model.table, role) {
//      override def setSessingUser(result: Result, user: Users#User)(implicit request: RequestHeader): Result = result
//    }
//    h2(entityId)
//  }
//
//  "GetHandler" should {
//    "return the correct entity" in {
//      val result = call(getHandler(cases, aCase.id), FakeRequest())
//      contentAsJson(result) must be equalTo aCase.toJson
//    }
//
//    "reject access if user hasn't the sufficient rights" in {
//      val result = call(getHandler(cases, aCase.id, "SUPERADMIN"), FakeRequest())
//      result.await must throwAn[AuthorizationError]
//    }
//
//    "hide sensitive attributes of entity" in {
//      val user = users.getEntity("user_login", None, Json.obj("login" -> "user_login", "name" -> "an user", "roles" -> List("READ"), "@password" -> "password_hash"))
//      val result = call(getHandler(users, user.id), FakeRequest())
//      contentAsJson(result) must be equalTo user.toJson - "@password"
//    }
//  }
//}