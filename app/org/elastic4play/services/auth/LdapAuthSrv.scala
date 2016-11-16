package org.elastic4play.services.auth

import java.util.Hashtable

import javax.inject.{ Inject, Singleton }
import javax.naming.Context
import javax.naming.directory.{ BasicAttribute, DirContext, InitialDirContext, ModificationItem, SearchControls }

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

import play.api.{ Configuration, Logger }
import play.api.mvc.RequestHeader

import org.elastic4play.{ AuthenticationError, AuthorizationError }
import org.elastic4play.services.{ AuthCapability, AuthContext, AuthSrv, AuthSrvFactory, UserSrv }

@Singleton
class LdapAuthSrvFactory @Inject() (
  configuration: Configuration,
    userSrv: UserSrv,
    ec: ExecutionContext
) extends AuthSrvFactory { factory ⇒
  val name = "ldap"
  def getAuthSrv: AuthSrv = new LdapAuthSrv(
    configuration.getString("auth.ldap.serverName").getOrElse(sys.error("Configuration error (auth.ldap.serverName is missing)")),
    configuration.getBoolean("auth.ldap.useSSL").getOrElse(false),
    configuration.getString("auth.ldap.bindDN").getOrElse(sys.error("Configuration error (auth.ldap.bindDN is missing)")),
    configuration.getString("auth.ldap.bindPW").getOrElse(sys.error("Configuration error (auth.ldap.bindPW is missing)")),
    configuration.getString("auth.ldap.baseDN").getOrElse(sys.error("Configuration error (auth.ldap.baseDN is missing)")),
    configuration.getString("auth.ldap.filter").getOrElse(sys.error("Configuration error (auth.ldap.filter is missing)")),
    userSrv,
    ec
  )

  private class LdapAuthSrv(
    serverName: String,
      useSSL: Boolean,
      bindDN: String,
      bindPW: String,
      baseDN: String,
      filter: String,
      userSrv: UserSrv,
      implicit val ec: ExecutionContext
  ) extends AuthSrv {

    lazy val log = Logger(getClass)
    val name = "ldap"
    val capabilities = Set(AuthCapability.changePassword)

    @Inject() def this(
      configuration: Configuration,
      userSrv: UserSrv,
      ec: ExecutionContext
    ) =
      this(
        configuration.getString("auth.ldap.serverName").getOrElse(sys.error("Configuration error (auth.ldap.serverName is missing)")),
        configuration.getBoolean("auth.ldap.useSSL").getOrElse(false),
        configuration.getString("auth.ldap.bindDN").getOrElse(sys.error("Configuration error (auth.ldap.bindDN is missing)")),
        configuration.getString("auth.ldap.bindPW").getOrElse(sys.error("Configuration error (auth.ldap.bindPW is missing)")),
        configuration.getString("auth.ldap.baseDN").getOrElse(sys.error("Configuration error (auth.ldap.baseDN is missing)")),
        configuration.getString("auth.ldap.filter").getOrElse(sys.error("Configuration error (auth.ldap.filter is missing)")),
        userSrv,
        ec
      )

    private[auth] def connect[A](username: String, password: String)(f: InitialDirContext ⇒ A): Try[A] = {
      val protocol = if (useSSL) "ldaps://" else "ldap://"
      val env = new Hashtable[Any, Any]
      env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory")
      env.put(Context.PROVIDER_URL, protocol + serverName)
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
        val searchResult = ctx.search(baseDN, filter, Array[Object](username), controls)
        if (searchResult.hasMore()) searchResult.next().getNameInNamespace
        else throw AuthenticationError("User not found in LDAP server")
      }
    }

    def authenticate(username: String, password: String)(implicit request: RequestHeader): Future[AuthContext] = {
      connect(bindDN, bindPW) { ctx ⇒
        getUserDN(ctx, username)
      }
        .flatten
        .flatMap { userDN ⇒
          connect(userDN, password) { ctx ⇒
            userSrv.get(username)
              .flatMap { u ⇒ userSrv.getFromUser(request, u) }
          }
        }
        .recover { case t ⇒ Future.failed(t) }
        .get
        .recoverWith {
          case t ⇒
            log.error("LDAP authentication failure", t)
            Future.failed(AuthenticationError("Authentication failure"))
        }
    }

    def changePassword(username: String, oldPassword: String, newPassword: String)(implicit authContext: AuthContext): Future[Unit] = {
      val changeTry = connect(bindDN, bindPW) { ctx ⇒
        getUserDN(ctx, username)
      }
        .flatten
        .flatMap { userDN ⇒
          connect(userDN, oldPassword) { ctx ⇒
            val mods = Array(new ModificationItem(DirContext.REPLACE_ATTRIBUTE, new BasicAttribute("userPassword", newPassword)))
            ctx.modifyAttributes(userDN, mods)
          }
        }
      Future
        .fromTry(changeTry)
        .recoverWith {
          case t ⇒
            log.error("LDAP change password failure", t)
            Future.failed(AuthorizationError("Change password failure"))
        }
    }

    def setPassword(username: String, newPassword: String)(implicit authContext: AuthContext): Future[Unit] = Future.failed(AuthorizationError("Operation not supported"))
  }
}