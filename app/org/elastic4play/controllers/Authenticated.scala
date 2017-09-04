package org.elastic4play.controllers

import java.util.Date
import javax.inject.{ Inject, Singleton }

import scala.concurrent.duration.{ DurationLong, FiniteDuration }
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

import play.api.{ Configuration, Logger }
import play.api.http.HeaderNames
import play.api.mvc._

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
  def roles: Seq[Role] = authContext.roles
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
    sessionUsername: String,
    userSrv: UserSrv,
    authSrv: AuthSrv,
    defaultParser: BodyParsers.Default,
    implicit val ec: ExecutionContext) {

  @Inject() def this(
    configuration: Configuration,
    userSrv: UserSrv,
    authSrv: AuthSrv,
    defaultParser: BodyParsers.Default,
    ec: ExecutionContext) =
    this(
      configuration.getMillis("session.inactivity").millis,
      configuration.getMillis("session.warning").millis,
      configuration.getOptional[String]("session.username").getOrElse("username"),
      userSrv,
      authSrv,
      defaultParser,
      ec)

  private[Authenticated] lazy val logger = Logger(getClass)
  private def now = (new Date).getTime

  /**
   * Insert or update session cookie containing user name and session expiration timestamp
   * Cookie is signed by Play framework (it cannot be modified by user)
   */
  def setSessingUser(result: Result, authContext: AuthContext)(implicit request: RequestHeader): Result =
    result.addingToSession(sessionUsername → authContext.userId, "expire" → (now + maxSessionInactivity.toMillis).toString)

  /**
   * Retrieve authentication information form cookie
   */
  def getFromSession(request: RequestHeader): Future[AuthContext] = {
    val userId = for {
      userId ← request.session.get(sessionUsername).toRight(AuthenticationError("User session not found"))
      _ = if (expirationStatus(request) != ExpirationError) Right(()) else Left(AuthenticationError("User session has expired"))
    } yield userId
    userId.fold(authError ⇒ Future.failed[AuthContext](authError), id ⇒ userSrv.getFromId(request, id))
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
    for {
      auth ← request
        .headers
        .get(HeaderNames.AUTHORIZATION)
        .fold(Future.failed[String](AuthenticationError("Authentication header not found")))(Future.successful)
      _ ← if (!auth.startsWith("Bearer ")) Future.failed(AuthenticationError("Only bearer authentication is supported")) else Future.successful(())
      key = auth.substring(7)
      authContext ← authSrv.authenticate(key)(request)
    } yield authContext

  def getFromBasicAuth(request: RequestHeader): Future[AuthContext] =
    for {
      auth ← request
        .headers
        .get(HeaderNames.AUTHORIZATION)
        .fold(Future.failed[String](AuthenticationError("Authentication header not found")))(Future.successful)
      _ ← if (!auth.startsWith("Basic ")) Future.failed(AuthenticationError("Only basic authentication is supported")) else Future.successful(())
      authWithoutBasic = auth.substring(6)
      decodedAuth = new String(java.util.Base64.getDecoder.decode(authWithoutBasic), "UTF-8")
      authContext ← decodedAuth.split(":") match {
        case Array(username, password) ⇒ authSrv.authenticate(username, password)(request)
        case _                         ⇒ Future.failed(AuthenticationError("Can't decode authentication header"))
      }
    } yield authContext

  /**
   * Get user in session -orElse- get user from key parameter
   */
  def getContext(request: RequestHeader): Future[AuthContext] =
    getFromSession(request).recoverWith {
      case getFromSessionError ⇒
        getFromApiKey(request).recoverWith {
          case getFromApiKeyError ⇒
            getFromBasicAuth(request).recoverWith {
              case getFromBasicAuthError ⇒
                userSrv.getInitialUser(request).recoverWith {
                  case getInitialUserError ⇒
                    logger.error(
                      s"""Authentication error:
                       |  From session   : ${getFromSessionError.getClass.getSimpleName} ${getFromSessionError.getMessage}
                       |  From api key   : ${getFromApiKeyError.getClass.getSimpleName} ${getFromApiKeyError.getMessage}
                       |  From basic auth: ${getFromBasicAuthError.getClass.getSimpleName} ${getFromBasicAuthError.getMessage}
                       |  Initial user   : ${getInitialUserError.getClass.getSimpleName} ${getInitialUserError.getMessage}
                     """.stripMargin)
                    Future.failed(AuthenticationError("Not authenticated"))
                }
            }
        }
    }

  /**
   * Create an action for authenticated controller
   * If user has sufficient right (have required role) action is executed
   * otherwise, action returns a not authorized error
   */
  def apply(requiredRole: Role) = new ActionBuilder[AuthenticatedRequest, AnyContent] {
    val executionContext: ExecutionContext = ec

    def parser: BodyParser[AnyContent] = defaultParser

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