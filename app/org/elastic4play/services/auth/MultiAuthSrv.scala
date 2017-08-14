package org.elastic4play.services.auth

import javax.inject.{ Inject, Singleton }

import scala.collection.immutable
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Try }

import play.api.mvc.RequestHeader
import play.api.{ Configuration, Logger }

import org.elastic4play.AuthenticationError
import org.elastic4play.services.AuthCapability.Type
import org.elastic4play.services.{ AuthContext, AuthSrv, AuthSrvFactory }

object MultiAuthSrv {
  lazy val log = Logger(classOf[MultiAuthSrv])
  def getAuthProviders(
    authTypes: Seq[String],
    authModules: immutable.Set[AuthSrv],
    authFactoryModules: immutable.Set[AuthSrvFactory]): Seq[AuthSrv] = {

    authTypes.flatMap { authType ⇒
      authFactoryModules
        .find(_.name == authType)
        .flatMap { authFactory ⇒
          Try(authFactory.getAuthSrv)
            .recoverWith {
              case error ⇒
                log.error(s"Initialization of authentication module $authType has failed", error)
                Failure(error)
            }
            .toOption
        }
        .orElse(authModules.find(_.name == authType))
        .orElse {
          log.error(s"Authentication module $authType not found")
          None
        }
    }
  }
}

@Singleton
class MultiAuthSrv(
    val authProviders: Seq[AuthSrv],
    implicit val ec: ExecutionContext) extends AuthSrv {

  lazy val log = Logger(getClass)

  @Inject() def this(
    configuration: Configuration,
    authModules: immutable.Set[AuthSrv],
    authFactoryModules: immutable.Set[AuthSrvFactory],
    ec: ExecutionContext) =
    this(
      MultiAuthSrv.getAuthProviders(
        configuration.getOptional[Seq[String]]("auth.type").getOrElse(Seq("local")),
        authModules,
        authFactoryModules),
      ec)

  val name = "multi"
  def capabilities: Set[Type] = authProviders.flatMap(_.capabilities).toSet

  private[auth] def forAllAuthProvider[A](body: AuthSrv ⇒ Future[A]) = {
    authProviders.foldLeft(Future.failed[A](new Exception("no authentication provider found"))) {
      (f, a) ⇒ f.recoverWith { case _ ⇒ body(a) }
    }
  }

  def authenticate(username: String, password: String)(implicit request: RequestHeader): Future[AuthContext] =
    forAllAuthProvider(_.authenticate(username, password))
      .recoverWith { case _ ⇒ Future.failed(AuthenticationError("Authentication failure")) }

  def changePassword(username: String, oldPassword: String, newPassword: String)(implicit authContext: AuthContext): Future[Unit] =
    forAllAuthProvider(_.changePassword(username, oldPassword, newPassword))

  def setPassword(username: String, newPassword: String)(implicit authContext: AuthContext): Future[Unit] =
    forAllAuthProvider(_.setPassword(username, newPassword))
}