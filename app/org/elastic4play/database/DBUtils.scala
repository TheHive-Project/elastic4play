package org.elastic4play.database

import com.sksamuel.elastic4s.http.ElasticDsl.fieldSort
import com.sksamuel.elastic4s.http.ElasticError
import com.sksamuel.elastic4s.http.search.SearchHit
import com.sksamuel.elastic4s.searches.sort.FieldSortDefinition
import com.sksamuel.elastic4s.{ Hit, HitReader }
import org.elastic4play.{ SearchError, utils }
import org.elasticsearch.index.IndexNotFoundException
import org.elasticsearch.transport.RemoteTransportException
import play.api.libs.json._

object DBUtils {
  def sortDefinition(sortBy: Seq[String]): Seq[FieldSortDefinition] = {
    import com.sksamuel.elastic4s.searches.sort.SortOrder._
    val byFieldList: Seq[(String, FieldSortDefinition)] = sortBy
      .map {
        case f if f.startsWith("+") ⇒ f.drop(1) → fieldSort(f.drop(1)).order(ASC)
        case f if f.startsWith("-") ⇒ f.drop(1) → fieldSort(f.drop(1)).order(DESC)
        case f if f.length() > 0    ⇒ f → fieldSort(f)
      }
    // then remove duplicates
    // Same as : val fieldSortDefs = byFieldList.groupBy(_._1).map(_._2.head).values.toSeq
    utils.Collection
      .distinctBy(byFieldList)(_._1)
      .map(_._2) :+ fieldSort("_uid").order(DESC)
  }

  /**
    * Transform search hit into JsObject
    * This function parses hit source add _type, _routing, _parent and _id attributes
    */
  implicit object JsonHitReader extends HitReader[JsObject] {
    def read(hit: Hit): Either[Throwable, JsObject] = hit match {
      case h: SearchHit ⇒
        val routing = h.routing.getOrElse(h.id)
        val parent = h.parent.fold[JsValue](JsNull)(JsString.apply)
        val source = toJson(h.sourceAsMap).as[JsObject]
        Right(source +
          ("_type" → JsString(h.`type`)) +
          ("_routing" → JsString(routing)) +
          ("_parent" → parent) +
          ("_version" -> JsNumber(hit.version)) +
          ("_id" → JsString(hit.id)))
    }
  }

  def toJson(value: Any): JsValue = value match {
    case obj: Map[_, _] ⇒ JsObject(obj.map(kv ⇒ kv._1.toString -> toJson(kv._2)))
    case s: String      ⇒ JsString(s)
    case n: Number      ⇒ JsNumber(n.doubleValue())
    case b: Boolean     ⇒ JsBoolean(b)
    case s: Seq[_]      ⇒ JsArray(s.map(toJson))
    case null           ⇒ JsNull
    case o ⇒
      println(s"*** ERROR *** : unexpected value for toJson : $o (${o.getClass})")
      JsNull
  }

  @scala.annotation.tailrec
  def isIndexMissing(t: Throwable): Boolean = t match {
    case SearchError(_, _, ElasticError("index_not_found_exception", _, _, _, _, _)) ⇒ true
    case t: RemoteTransportException ⇒ isIndexMissing(t.getCause)
    case _: IndexNotFoundException ⇒ true
    case _ ⇒ false
  }
}