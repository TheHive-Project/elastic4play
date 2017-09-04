package org.elastic4play.services.auth

import java.util
import javax.inject.{ Inject, Singleton }
import javax.naming.Context
import javax.naming.directory._

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Success, Try }

import play.api.mvc.RequestHeader
import play.api.{ Configuration, Logger }

import org.elastic4play.services.{ AuthCapability, _ }
import org.elastic4play.{ AuthenticationError, AuthorizationError }

@Singleton
class LdapAuthSrvFactory @Inject() (
    configuration: Configuration,
    userSrv: UserSrv,
    ec: ExecutionContext) extends AuthSrvFactory {
  val name = "ldap"

  def getAuthSrv: AuthSrv = new LdapAuthSrv(
    configuration.get[String]("auth.ldap.serverName"),
    configuration.getOptional[Boolean]("auth.ldap.useSSL").getOrElse(false),
    configuration.get[String]("auth.ldap.bindDN"),
    configuration.get[String]("auth.ldap.bindPW"),
    configuration.get[String]("auth.ldap.baseDN"),
    configuration.get[String]("auth.ldap.filter"),
    userSrv,
    ec)

  private class LdapAuthSrv(
      serverName: String,
      useSSL: Boolean,
      bindDN: String,
      bindPW: String,
      baseDN: String,
      filter: String,
      userSrv: UserSrv,
      implicit val ec: ExecutionContext) extends AuthSrv {

    private[LdapAuthSrv] lazy val logger = Logger(getClass)
    val name = "ldap"
    val capabilities = Set(AuthCapability.changePassword)

    private[auth] def connect[A](username: String, password: String)(f: InitialDirContext ⇒ Try[A]): Try[A] = {
      val protocol = if (useSSL) "ldaps://" else "ldap://"
      val env = new util.Hashtable[Any, Any]
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
        .flatten
    }

    private[auth] def getUserDN(ctx: InitialDirContext, username: String): Try[String] = {
      Try {
        val controls = new SearchControls()
        controls.setSearchScope(SearchControls.SUBTREE_SCOPE)
        controls.setCountLimit(1)
        val searchResult = ctx.search(baseDN, filter, Array[Object](username), controls)
        if (searchResult.hasMore) searchResult.next().getNameInNamespace
        else throw AuthenticationError("User not found in LDAP server")
      }
    }

    private[auth] def getUserNameFromKey(ctx: InitialDirContext, key: String): Try[String] = {
      Try {
        val controls = new SearchControls()
        controls.setSearchScope(SearchControls.SUBTREE_SCOPE)
        controls.setCountLimit(1)
        val searchResult = ctx.search(baseDN, keyFilter.getOrElse(throw AuthenticationError("Authentication by key is not possible as auth.ldap.keyFilter is not configured")), Array[Object](key), controls)
        if (searchResult.hasMore) searchResult.next().getAttributes.get(loginAttribute).get().toString
        else throw AuthenticationError("User not found in LDAP server")
      }
    }

    override def authenticate(username: String, password: String)(implicit request: RequestHeader): Future[AuthContext] = {
      connect(bindDN, bindPW) { ctx ⇒
        getUserDN(ctx, username)
      }
        .flatMap { userDN ⇒ connect(userDN, password)(_ ⇒ Success(())) }
        .map { _ ⇒
          userSrv.get(username)
            .flatMap { u ⇒ userSrv.getFromUser(request, u) }
        }
        .fold[Future[AuthContext]](Future.failed, identity)
        .recoverWith {
          case t ⇒
            logger.error("LDAP authentication failure", t)
            Future.failed(AuthenticationError("Authentication failure"))
        }
    }

    override def authenticate(key: String)(implicit request: RequestHeader): Future[AuthContext] = {
      keyFilter
        .map { _ ⇒
          connect(bindDN, bindPW) { ctx ⇒
            getUserNameFromKey(ctx, key)
          }
            .map(username ⇒ userSrv.getFromId(request, username))
            .fold(Future.failed, identity)
            .recoverWith {
              case t ⇒
                logger.error("LDAP authentication failure", t)
                Future.failed(AuthenticationError("Authentication failure"))
            }
        }
        .getOrElse(Future.failed(AuthorizationError("ldap authenticator doesn't support api key authentication")))
    }

    override def changePassword(username: String, oldPassword: String, newPassword: String)(implicit authContext: AuthContext): Future[Unit] = {
      connect(bindDN, bindPW) { ctx ⇒
        getUserDN(ctx, username)
      }
        .flatMap { userDN ⇒
          connect(userDN, oldPassword) { ctx ⇒
            val mods = Array(new ModificationItem(DirContext.REPLACE_ATTRIBUTE, new BasicAttribute("userPassword", newPassword)))
            Try(ctx.modifyAttributes(userDN, mods))
          }
        }
        .fold(Future.failed, Future.successful)
        .recoverWith {
          case t ⇒
            logger.error("LDAP change password failure", t)
            Future.failed(AuthorizationError("Change password failure"))
        }
    }

    override def renewKey(username: String)(implicit authContext: AuthContext): Future[String] = {
      keyAttribute.map { ka ⇒
        connect(bindDN, bindPW) { ctx ⇒
          getUserDN(ctx, username).flatMap { userDN ⇒
            val newKey = generateKey()
            val mods = Array(new ModificationItem(DirContext.REPLACE_ATTRIBUTE, new BasicAttribute(ka, newKey)))
            Try(ctx.modifyAttributes(userDN, mods)).map(_ ⇒ newKey)
          }
        }
          .fold(Future.failed, Future.successful)
          .recoverWith {
            case t ⇒
              logger.error("LDAP renew key failure", t)
              Future.failed(AuthorizationError("Renew key failure"))
          }
      }
        .getOrElse(Future.failed(AuthorizationError("Operation not supported")))
    }

    override def getKey(username: String)(implicit authContext: AuthContext): Future[String] = {
      keyAttribute.map { ka ⇒
        connect(bindDN, bindPW) { ctx ⇒
          Try {
            val controls = new SearchControls()
            controls.setSearchScope(SearchControls.SUBTREE_SCOPE)
            controls.setCountLimit(1)
            val searchResult = ctx.search(baseDN, filter, Array[Object](username), controls)
            if (searchResult.hasMore) {
              searchResult.next().getAttributes.get(ka).get().toString
            }
            else throw AuthenticationError("User not found in LDAP server")
          }
        }
          .fold(Future.failed, Future.successful)
          .recoverWith {
            case t ⇒
              logger.error("LDAP renew key failure", t)
              Future.failed(AuthorizationError("Renew key failure"))
          }
      }
        .getOrElse(Future.failed(AuthorizationError("Operation not supported")))
    }
  }
}