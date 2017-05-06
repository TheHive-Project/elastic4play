package org.elastic4play.services

import java.util.concurrent.atomic.AtomicBoolean

import scala.annotation.implicitNotFound
import scala.concurrent.Future

import play.api.libs.json.JsObject
import play.api.mvc.RequestHeader

import org.elastic4play.models.HiveEnumeration

object Role extends Enumeration with HiveEnumeration {
  type Type = Value
  val read, write, admin = Value
}

trait AuthContext {
  def userId: String
  def userName: String
  def requestId: String
  def roles: Seq[Role.Type]
  private val baseAudit = new AtomicBoolean(true)
  def getBaseAudit(): Boolean = baseAudit.get && baseAudit.getAndSet(false)
}

trait UserSrv {
  def getFromId(request: RequestHeader, userId: String): Future[AuthContext]
  def getFromUser(request: RequestHeader, user: User): Future[AuthContext]
  def getInitialUser(request: RequestHeader): Future[AuthContext]
  def inInitAuthContext[A](block: AuthContext ⇒ Future[A]): Future[A]
  def get(userId: String): Future[User]
}

trait User {
  val attributes: JsObject
  val id: String
  def getUserName: String
  def getRoles: Seq[Role.Type]
}

object AuthCapability extends Enumeration {
  type Type = Value
  val changePassword, setPassword = Value
}
trait AuthSrv {
  val name: String
  def capabilities: Set[AuthCapability.Type]
  def authenticate(username: String, password: String)(implicit request: RequestHeader): Future[AuthContext]
  def changePassword(username: String, oldPassword: String, newPassword: String)(implicit authContext: AuthContext): Future[Unit]
  def setPassword(username: String, newPassword: String)(implicit authContext: AuthContext): Future[Unit]
}
trait AuthSrvFactory {
  val name: String
  def getAuthSrv: AuthSrv
}