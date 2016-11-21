package org.elastic4play.controllers

import javax.inject.{ Inject, Singleton }

import scala.concurrent.ExecutionContext
import scala.reflect.runtime.universe

import play.api.libs.json.JsValue
import play.api.mvc.Controller

import org.elastic4play.Timed
import org.elastic4play.services.{ DBLists, Role }
import scala.concurrent.Future

@Singleton
class DBListCtrl @Inject() (
    dblists: DBLists,
    authenticated: Authenticated,
    renderer: Renderer,
    fieldsBodyParser: FieldsBodyParser,
    implicit val ec: ExecutionContext) extends Controller {

  @Timed("controllers.DBListCtrl.list")
  def list = authenticated(Role.read).async { implicit request ⇒
    dblists.listAll.map { listNames ⇒
      renderer.toOutput(OK, listNames)
    }
  }

  @Timed("controllers.DBListCtrl.listItems")
  def listItems(listName: String) = authenticated(Role.read) { implicit request ⇒
    val (src, total) = dblists(listName).getItems[JsValue]
    val items = src.map { case (id, value) ⇒ s""""$id":$value""" }
      .intersperse("{", ",", "}")
    Ok.chunked(items).as("application/json")
  }

  @Timed("controllers.DBListCtrl.addItem")
  def addItem(listName: String) = authenticated(Role.admin).async(fieldsBodyParser) { implicit request ⇒
    request.body.getValue("value").fold(Future.successful(NoContent)) { value ⇒
      dblists(listName).addItem(value).map { item ⇒
        renderer.toOutput(OK, item.id)
      }
    }
  }

  @Timed("controllers.DBListCtrl.deleteItem")
  def deleteItem(itemId: String) = authenticated(Role.admin).async { implicit request ⇒
    dblists.deleteItem(itemId).map { _ ⇒
      NoContent
    }
  }
}