package org.elastic4play.controllers

import javax.inject.Inject

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

import akka.stream.Materializer
import akka.stream.scaladsl.Source

import play.api.http.Status
import play.api.libs.json.{ JsArray, JsObject, Json }
import play.api.libs.json.Json.toJsFieldJsValueWrapper
import play.api.libs.json.Writes
import play.api.libs.json.Writes.{ JsValueWrites, OptionWrites, StringWrites, traversableWrites }
import play.api.mvc.{ AcceptExtractors, Rendering, Request, Result, Results }

import org.elastic4play.{ AttributeCheckingError, CreateError, UpdateError }

class Renderer @Inject() (implicit
  val ec: ExecutionContext,
    implicit val mat: Materializer) extends Rendering with AcceptExtractors {

  //  private def getTypeName[C](content: C)(implicit t: u.TypeTag[C]): String = {
  //    val tpe = t.tpe
  //    val m = u.runtimeMirror(content.getClass.getClassLoader)
  //    if (tpe <:< scala.reflect.runtime.universe.typeOf[Seq[_]])
  //      m.runtimeClass(tpe.asInstanceOf[u.TypeRefApi].args.head.typeSymbol.asClass).getSimpleName + "List"
  //    else
  //      m.runtimeClass(tpe.typeSymbol.asClass).getSimpleName
  //  }

  def toMultiOutput[A](status: Int, objects: Seq[Try[A]])(implicit writes: Writes[A], request: Request[_]): Result = {
    val (success, failure) = objects.foldLeft((Seq.empty[A], Seq.empty[JsObject])) {
      case ((artifacts, errors), Success(a))                                         ⇒ (a +: artifacts, errors)
      case ((artifacts, errors), Failure(CreateError(status, message, obj)))         ⇒ (artifacts, Json.obj("object" → obj, "type" → status, "error" → message) +: errors)
      case ((artifacts, errors), Failure(UpdateError(status, message, obj)))         ⇒ (artifacts, Json.obj("object" → obj, "type" → status, "error" → message) +: errors)
      case ((artifacts, errors), Failure(AttributeCheckingError(table, attrErrors))) ⇒ (artifacts, Json.obj("type" → "AttributeError", "errors" → attrErrors.map(_.toString)) +: errors)
      case ((artifacts, errors), Failure(t))                                         ⇒ (artifacts, Json.obj("error" → t.getMessage) +: errors)
    }
    if (failure.isEmpty)
      toOutput(status, success)
    else if (success.isEmpty)
      toOutput(Status.BAD_REQUEST, JsArray(failure))
    else
      toOutput(Status.MULTI_STATUS, Json.obj("success" → success, "failure" → failure))
  }
  /**
   * Render "content" object regarding the expected format in HTTP request : XML, HTML or JSON (default)
   */
  //  def toOutput[C](status: Int, content: C)(implicit writes: Writes[C], request: Request[_], contentType: u.TypeTag[C]): Result = {
  //    toOutput(status, getTypeName(content), content)
  //  }

  def toOutput[C](status: Int, /* returnType: String,*/ content: C)(implicit writes: Writes[C], request: Request[_]): Result = {
    val json = Json.toJson(content)
    val s = new Results.Status(status)
    val output = render {
      case Accepts.Json() ⇒
        s(json)
      //      case Accepts.Html() =>
      //        Ok(views.html.output(returnType)(Html(toPrettyString(toHtml(returnType)(json)))))
      //      case Accepts.Xml() =>
      //        s(toXml(returnType)(json))
    }
    if (output.header.status == Status.NOT_ACCEPTABLE)
      s(json)
    else
      output
  }

  def toOutput[C](status: Int, src: Source[C, _], total: Future[Long])(implicit writes: Writes[C], request: Request[_]): Future[Result] = {
    val stringSource = src.map(_.toString).intersperse("[", ",", "]")
    total.map { t ⇒
      new Results.Status(status).chunked(stringSource)
        .withHeaders("X-Total" → t.toString)
    }
  }
}
