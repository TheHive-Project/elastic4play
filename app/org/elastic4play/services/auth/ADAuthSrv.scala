package org.elastic4play.services.auth

import java.util
import javax.inject.{ Inject, Singleton }
import javax.naming.Context
import javax.naming.directory._

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

import play.api.mvc.RequestHeader
import play.api.{ Configuration, Logger }

import org.elastic4play.services._
import org.elastic4play.{ AuthenticationError, AuthorizationError }

@Singleton
class ADAuthSrvFactory @Inject() (
    configuration: Configuration,
    userSrv: UserSrv,
    ec: ExecutionContext) extends AuthSrvFactory { factory ⇒
  val name = "ad"
  def getAuthSrv: AuthSrv = new ADAuthSrv(
    configuration.get[String]("auth.ad.domainFQDN"),
    configuration.get[String]("auth.ad.domainName"),
    configuration.getOptional[Boolean]("auth.ad.useSSL").getOrElse(false),
    userSrv,
    ec)

  private class ADAuthSrv(
      DomainFQDN: String,
      domainName: String,
      useSSL: Boolean,
      userSrv: UserSrv,
      implicit val ec: ExecutionContext) extends AuthSrv {

    private[ADAuthSrv] lazy val logger = Logger(getClass)
    val name: String = factory.name
    override val capabilities: Set[AuthCapability.Value] = Set(AuthCapability.changePassword)

    private[auth] def connect[A](username: String, password: String)(f: InitialDirContext ⇒ A): Try[A] = {
      val protocol = if (useSSL) "ldaps://" else "ldap://"
      val env = new util.Hashtable[Any, Any]
      env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory")
      env.put(Context.PROVIDER_URL, protocol + DomainFQDN)
      env.put(Context.SECURITY_AUTHENTICATION, "simple")
      env.put(Context.SECURITY_PRINCIPAL, username)
      env.put(Context.SECURITY_CREDENTIALS, password)
      Try {
        val ctx = new InitialDirContext(env)
        try f(ctx)
        finally ctx.close()
      }
    }

    private[auth] def getUserDN(ctx: InitialDirContext, username: String): Try[String] = {
      Try {
        val controls = new SearchControls()
        controls.setSearchScope(SearchControls.SUBTREE_SCOPE)
        controls.setCountLimit(1)
        val domainDN = DomainFQDN.split("\\.").mkString("dc=", ",dc=", "")
        val searchResult = ctx.search(domainDN, "(sAMAccountName={0})", Array[Object](username), controls)
        if (searchResult.hasMore) searchResult.next().getNameInNamespace
        else throw AuthenticationError("User not found in Active Directory")
      }
    }

    override def authenticate(username: String, password: String)(implicit request: RequestHeader): Future[AuthContext] = {
      (for {
        _ ← Future.fromTry(connect(domainName + "\\" + username, password)(identity))
        u ← userSrv.get(username)
        authContext ← userSrv.getFromUser(request, u)
      } yield authContext)
        .recoverWith {
          case t ⇒
            logger.error("AD authentication failure", t)
            Future.failed(AuthenticationError("Authentication failure"))
        }
    }

    override def changePassword(username: String, oldPassword: String, newPassword: String)(implicit authContext: AuthContext): Future[Unit] = {
      val unicodeOldPassword = ("\"" + oldPassword + "\"").getBytes("UTF-16LE")
      val unicodeNewPassword = ("\"" + newPassword + "\"").getBytes("UTF-16LE")
      val changeTry = connect(domainName + "\\" + username, oldPassword) { ctx ⇒
        getUserDN(ctx, username).map { userDN ⇒
          val mods = Array(
            new ModificationItem(DirContext.REMOVE_ATTRIBUTE, new BasicAttribute("unicodePwd", unicodeOldPassword)),
            new ModificationItem(DirContext.ADD_ATTRIBUTE, new BasicAttribute("unicodePwd", unicodeNewPassword)))
          ctx.modifyAttributes(userDN, mods)
        }
      }
        .flatMap(identity)
      Future
        .fromTry(changeTry)
        .recoverWith {
          case t ⇒
            logger.error("LDAP change password failure", t)
            Future.failed(AuthorizationError("Change password failure"))
        }
    }
  }
}