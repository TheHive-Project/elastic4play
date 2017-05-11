package org.elastic4play

import scala.annotation.implicitNotFound
import scala.concurrent.Future

import play.api.Logger
import play.api.http.HttpErrorHandler
import play.api.libs.json.Json
import play.api.mvc.{ RequestHeader, Result, Results }

import org.elasticsearch.client.transport.NoNodeAvailableException
import org.elasticsearch.index.IndexNotFoundException
import org.elasticsearch.transport.RemoteTransportException

import org.elastic4play.JsonFormat.attributeCheckingExceptionWrites
import play.api.libs.json.JsValue
import play.api.http.Status
import play.api.libs.json.JsNull
import play.api.mvc.ResponseHeader
import play.api.http.Writeable

/**
 * This class handles errors. It traverses all causes of exception to find known error and shows the appropriate message
 */
class ErrorHandler extends HttpErrorHandler {

  def onClientError(request: RequestHeader, statusCode: Int, message: String): Future[Result] = Future.successful {
    Results.Status(statusCode)(s"A client error occurred on ${request.method} ${request.uri} : $message")
  }

  def toErrorResult(ex: Throwable): Option[(Int, JsValue)] = {
    ex match {
      case AuthenticationError(message)             ⇒ Some(Status.UNAUTHORIZED → Json.obj("type" → "AuthenticationError", "message" → message))
      case AuthorizationError(message)              ⇒ Some(Status.FORBIDDEN → Json.obj("type" → "AuthorizationError", "message" → message))
      case UpdateError(status, message, attributes) ⇒ Some(Status.INTERNAL_SERVER_ERROR → Json.obj("type" → "UpdateError", "message" → message, "object" → attributes))
      case InternalError(message)                   ⇒ Some(Status.INTERNAL_SERVER_ERROR → Json.obj("type" → "InternalError", "message" → message))
      case nfe: NumberFormatException               ⇒ Some(Status.BAD_REQUEST → Json.obj("type" → "NumberFormatException", "message" → ("Invalid format " + nfe.getMessage)))
      case NotFoundError(message)                   ⇒ Some(Status.NOT_FOUND → Json.obj("type" → "NotFoundError", "message" → message))
      case BadRequestError(message)                 ⇒ Some(Status.BAD_REQUEST → Json.obj("type" → "BadRequest", "message" → message))
      case SearchError(message, cause)              ⇒ Some(Status.BAD_REQUEST → Json.obj("type" → "SearchError", "message" → s"$message (${cause.getMessage})"))
      case ace: AttributeCheckingError              ⇒ Some(Status.BAD_REQUEST → Json.toJson(ace))
      case iae: IllegalArgumentException            ⇒ Some(Status.BAD_REQUEST → Json.obj("type" → "IllegalArgument", "message" → iae.getMessage))
      case nnae: NoNodeAvailableException           ⇒ Some(Status.INTERNAL_SERVER_ERROR → Json.obj("type" → "NoNodeAvailable", "message" → "ElasticSearch cluster is unreachable"))
      case CreateError(status, message, attributes) ⇒ Some(Status.INTERNAL_SERVER_ERROR → Json.obj("type" → "CreateError", "message" → message, "object" → attributes))
      case ConflictError(message, attributes)       ⇒ Some(Status.BAD_REQUEST → Json.obj("type" → "ConflictError", "message" → message, "object" → attributes))
      case GetError(message)                        ⇒ Some(Status.INTERNAL_SERVER_ERROR → Json.obj("type" → "GetError", "message" → message))
      case MultiError(message, exceptions) ⇒
        val suberrors = exceptions.map(e ⇒ toErrorResult(e)).collect {
          case Some((s, j)) ⇒ j
        }
        Some(Status.MULTI_STATUS → Json.obj("type" → "MultiError", "error" → message, "suberrors" → suberrors))
      case rte: RemoteTransportException ⇒
        rte.getCause match {
          case infe: IndexNotFoundException ⇒ Some(520 → JsNull)
          case t: Throwable                 ⇒ Some(Status.INTERNAL_SERVER_ERROR → Json.obj("type" → t.getClass.getName, "error" → s"Database error : ${t.getMessage}"))
        }
      case t: Throwable ⇒ Option(t.getCause).flatMap(toErrorResult)
    }
  }

  def toResult[C](status: Int, c: C)(implicit writeable: Writeable[C]) = Result(header = ResponseHeader(status), body = writeable.toEntity(c))

  def onServerError(request: RequestHeader, exception: Throwable): Future[Result] = {
    val (status, body) = toErrorResult(exception).getOrElse(Status.INTERNAL_SERVER_ERROR → Json.obj("type" → exception.getClass.getName, "message" → exception.getMessage))
    Logger.info(s"${request.method} ${request.uri} returned $status", exception)
    Future.successful(toResult(status, body))
  }
}