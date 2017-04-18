package org.elastic4play.controllers

import javax.inject.Inject

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

import akka.stream.Materializer
import akka.stream.scaladsl.Source

import play.api.http.Status
import play.api.libs.json.{ JsValue, Json }
import play.api.libs.json.Json.toJsFieldJsValueWrapper
import play.api.libs.json.Writes
import play.api.mvc.{ Request, Result, Results }

import org.elastic4play.ErrorHandler

class Renderer @Inject() (
    errorHandler: ErrorHandler,
    implicit val ec: ExecutionContext,
    implicit val mat: Materializer) {

  def toMultiOutput[A](status: Int, objects: Seq[Try[A]])(implicit writes: Writes[A], request: Request[_]): Result = {

    val (success, failure) = objects.foldLeft((Seq.empty[JsValue], Seq.empty[JsValue])) {
      case ((artifacts, errors), Success(a)) ⇒ (Json.toJson(a) +: artifacts, errors)
      case ((artifacts, errors), Failure(e)) ⇒
        val errorJson = errorHandler.toErrorResult(e) match {
          case Some((_, j)) ⇒ j
          case None         ⇒ Json.obj("type" → e.getClass.getName, "error" → e.getMessage)
        }
        (artifacts, errorJson +: errors)

    }
    if (failure.isEmpty)
      toOutput(status, success)
    else if (success.isEmpty)
      toOutput(Status.BAD_REQUEST, failure)
    else
      toOutput(Status.MULTI_STATUS, Json.obj("success" → success, "failure" → failure))
  }

  def toOutput[C](status: Int, content: C)(implicit writes: Writes[C], request: Request[_]): Result = {
    val json = Json.toJson(content)
    val s = new Results.Status(status)
    s(json)
  }

  def toOutput[C](status: Int, src: Source[C, _], total: Future[Long])(implicit writes: Writes[C], request: Request[_]): Future[Result] = {
    val stringSource = src.map(_.toString).intersperse("[", ",", "]")
    total.map { t ⇒
      new Results.Status(status)
        .chunked(stringSource)
        .as("application/json")
        .withHeaders("X-Total" → t.toString)
    }
  }
}
