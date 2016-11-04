package org.elastic4play.services

import javax.inject.{ Inject, Provider, Singleton }

import scala.annotation.implicitNotFound
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.DurationInt

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{ Sink, Source }

import play.api.Configuration
import play.api.cache.CacheApi
import play.api.libs.json.{ JsObject, JsString, JsValue }
import play.api.libs.json.{ Reads, Writes }
import play.api.libs.json.JsValue.jsValueToJsLookup
import play.api.libs.json.Json
import play.api.libs.json.Json.toJsFieldJsValueWrapper

import org.elastic4play.database.DBCreate
import org.elastic4play.models.{ AttributeFormat => F, EntityDef, ModelDef }

import org.elastic4play.utils.{ Hasher, RichFuture }

@Singleton
class DBListModel(dblistName: String) extends ModelDef[DBListModel, DBListItemEntity](dblistName) { model =>
  @Inject def this(configuration: Configuration) = this(configuration.getString("dblist.name").get)

  val value = attribute("value", F.stringFmt, "Content of the dblist item")
  val dblist = attribute("dblist", F.stringFmt, "Name of the dblist")
  override def apply(attributes: JsObject) = new DBListItemEntity(this, attributes)

}

class DBListItemEntity(model: DBListModel, attributes: JsObject) extends EntityDef[DBListModel, DBListItemEntity](model, attributes) with DBListItem {
  def mapTo[T](implicit reads: Reads[T]) = Json.parse((attributes \ "value").as[String]).as[T]
  override def toJson = super.toJson - "value" + ("value" -> mapTo[JsValue])
}

trait DBListItem {
  def id: String
  def mapTo[A](implicit reads: Reads[A]): A
}

trait DBList {
  def cachedItems: Seq[DBListItem]
  def getItems(): (Source[DBListItem, NotUsed], Future[Long])
  def getItems[A](implicit reads: Reads[A]): (Source[(String, A), NotUsed], Future[Long])
  def addItem[A](item: A)(implicit writes: Writes[A]): Future[DBListItem]
}

@Singleton
class DBLists @Inject() (findSrv: FindSrv,
                         deleteSrv: Provider[DeleteSrv],
                         dbCreate: DBCreate,
                         dblistModel: DBListModel,
                         cache: CacheApi,
                         implicit val ec: ExecutionContext,
                         implicit val mat: Materializer) {
  /**
   * Returns list of all dblist name
   */
  def listAll: Future[collection.Set[String]] = {
    import QueryDSL._
    findSrv(dblistModel, any, groupByField("dblist", selectCount)).map(_.keys)
  }

  def deleteItem(itemId: String)(implicit authContext: AuthContext) = deleteSrv.get.realDelete[DBListModel, DBListItemEntity](dblistModel, itemId)

  def apply(name: String): DBList = new DBList {
    def cachedItems = cache.getOrElse(dblistModel.name + "_" + name, 10.seconds) {
      val (src, total) = getItems()
      src.runWith(Sink.seq).await
    }

    def getItems(): (Source[DBListItem, NotUsed], Future[Long]) = {
      import org.elastic4play.services.QueryDSL._
      findSrv[DBListModel, DBListItemEntity](dblistModel, "dblist" ~= name, Some("all"), Nil)
    }

    def getItems[A](implicit reads: Reads[A]): (Source[(String, A), NotUsed], Future[Long]) = {
      val (src, total) = getItems()
      val items = src.map(item => (item.id, item.mapTo[A]))
      (items, total)
    }

    def addItem[A](item: A)(implicit writes: Writes[A]): Future[DBListItem] = {
      val value = Json.toJson(item)
      val id = Hasher("MD5").fromString(value.toString).head.toString
      dbCreate(dblistModel.name, None, Json.obj("_id" -> id, "dblist" -> name, "value" -> JsString(value.toString)))
        .map(dblistModel(_))
    }
  }

}