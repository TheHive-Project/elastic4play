package org.elastic4play.controllers

import java.util.Date

import javax.inject.{ Inject, Singleton }

import scala.language.reflectiveCalls
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.{ DurationLong, FiniteDuration }
import scala.util.Try

import play.api.Configuration
import play.api.http.HeaderNames
import play.api.mvc.{ ActionBuilder, Request, RequestHeader, Result, Security, WrappedRequest }

import org.elastic4play.AuthenticationError
import org.elastic4play.services.{ AuthContext, AuthSrv, Role, UserSrv }
import org.elastic4play.utils.Instance

/**
 * A request with authentication information
 */
class AuthenticatedRequest[A](val authContext: AuthContext, request: Request[A]) extends WrappedRequest[A](request) with AuthContext {
  def userId: String = authContext.userId
  def userName: String = authContext.userName
  def requestId: String = Instance.getRequestId(request)
  def roles: Seq[Role.Type] = authContext.roles
}

sealed trait ExpirationStatus
case class ExpirationOk(duration: FiniteDuration) extends ExpirationStatus
case class ExpirationWarning(duration: FiniteDuration) extends ExpirationStatus
case object ExpirationError extends ExpirationStatus

/**
 * Check and manager user security (authentication and authorization)
 */
@Singleton
class Authenticated(
    maxSessionInactivity: FiniteDuration,
    sessionWarning: FiniteDuration,
    userSrv: UserSrv,
    authSrv: AuthSrv,
    implicit val ec: ExecutionContext) {

  @Inject() def this(
    configuration: Configuration,
    userSrv: UserSrv,
    authSrv: AuthSrv,
    ec: ExecutionContext) =
    this(
      configuration.getMilliseconds("session.inactivity").get.millis,
      configuration.getMilliseconds("session.warning").get.millis,
      userSrv,
      authSrv,
      ec)

  private def now = (new Date).getTime

  /**
   * Insert or update session cookie containing user name and session expiration timestamp
   * Cookie is signed by Play framework (it cannot be modified by user)
   */
  def setSessingUser(result: Result, authContext: AuthContext)(implicit request: RequestHeader): Result =
    result.addingToSession(Security.username → authContext.userId, "expire" → (now + maxSessionInactivity.toMillis).toString)

  /**
   * Retrieve authentication information form cookie
   */
  def getFromSession(request: RequestHeader): Future[AuthContext] = {
    val userId = for {
      userId ← request.session.get(Security.username)
      if expirationStatus(request) != ExpirationError
    } yield userId
    userId.fold(Future.failed[AuthContext](AuthenticationError("Not authenticated")))(id ⇒ userSrv.getFromId(request, id))
  }

  def expirationStatus(request: RequestHeader): ExpirationStatus = {
    request.session.get("expire")
      .flatMap { expireStr ⇒
        Try(expireStr.toLong).toOption
      }
      .map { expire ⇒ (expire - now).millis }
      .map {
        case duration if duration.length < 0       ⇒ ExpirationError
        case duration if duration < sessionWarning ⇒ ExpirationWarning(duration)
        case duration                              ⇒ ExpirationOk(duration)
      }
      .getOrElse(ExpirationError)
  }

  /**
   * Retrieve authentication information from API key
   */
  def getFromApiKey(request: RequestHeader): Future[AuthContext] =
    request
      .headers
      .get(HeaderNames.AUTHORIZATION)
      .collect {
        case auth if auth.startsWith("Basic ") ⇒
          val authWithoutBasic = auth.substring(6)
          val decodedAuth = new String(java.util.Base64.getDecoder.decode(authWithoutBasic), "UTF-8")
          decodedAuth.split(":")
      }
      .collect {
        case Array(username, password) ⇒ authSrv.authenticate(username, password)(request)
      }
      .getOrElse(Future.failed[AuthContext](new Exception("TODO")))

  /**
   * Get user in session -orElse- get user from key parameter
   */
  def getContext(request: RequestHeader): Future[AuthContext] =
    getFromSession(request)
      .fallbackTo(getFromApiKey(request))
      .fallbackTo(userSrv.getInitialUser(request))
      .recoverWith { case _ ⇒ Future.failed(AuthenticationError("Not authenticated")) }

  /**
   * Create an action for authenticated controller
   * If user has sufficient right (have required role) action is executed
   * otherwise, action returns a not authorized error
   */
  def apply(requiredRole: Role.Type) = new ActionBuilder[({ type R[A] = AuthenticatedRequest[A] })#R] {
    def invokeBlock[A](request: Request[A], block: (AuthenticatedRequest[A]) ⇒ Future[Result]): Future[Result] = {
      getContext(request).flatMap { authContext ⇒
        if (authContext.roles.contains(requiredRole))
          block(new AuthenticatedRequest(authContext, request))
            .map(result ⇒ setSessingUser(result, authContext)(request))
        else
          Future.failed(new Exception(s"Insufficient rights to perform this action"))
      }
    }
  }
}