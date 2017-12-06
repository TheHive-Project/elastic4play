package org.elastic4play.database

import scala.util.Try

import play.api.libs.json._

import com.sksamuel.elastic4s.http.ElasticDsl.fieldSort
import com.sksamuel.elastic4s.http.search.SearchHit
import com.sksamuel.elastic4s.{ Hit, HitReader }
import com.sksamuel.elastic4s.searches.sort.FieldSortDefinition
import org.elasticsearch.index.IndexNotFoundException
import org.elasticsearch.transport.RemoteTransportException

import org.elastic4play.{ InternalError, utils }

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
    def read(hit: Hit): Either[Throwable, JsObject] = {
      hit match {
        case h: SearchHit => h.sourceAsString
      }
      val fields = hit.sourceAsMap
      for {
        source ← Try(Json.parse(hit.sourceAsString).as[JsObject]).toEither
        _ = println(s"fields=$fields")
        routing ← fields.get("_routing")
          .collect { case r: String ⇒ r }
          .toRight(InternalError(s"routing is missing in hit $hit"))
        parent = fields.get("_parent").fold[JsValue](JsNull)(p ⇒ JsString(p.toString))
      } yield source +
        ("_type" → JsString(hit.`type`)) +
        ("_routing" → JsString(routing)) +
        ("_parent" → parent) +
        ("_version" -> JsNumber(hit.version)) +
        ("_id" → JsString(hit.id))
    }
  }

  //  def hit2json( /*fields: Option[Seq[Attribute[_]]], */ hit: RichSearchHit): JsObject = {
  //    val fieldsValue = hit.fields
  //    val id = JsString(hit.id)
  //    Option(hit.sourceAsString).filterNot(_ == "").fold(JsObject.empty)(s ⇒ Json.parse(s).as[JsObject]) +
  //      ("_type" → JsString(hit.`type`)) +
  //      ("_routing" → fieldsValue.get("_routing").map(r ⇒ JsString(r.java.getValue[String])).getOrElse(id)) +
  //      ("_parent" → fieldsValue.get("_parent").map(r ⇒ JsString(r.java.getValue[String])).getOrElse(JsNull)) +
  //      ("_id" → id)
  //  }

  @scala.annotation.tailrec
  def isIndexMissing(t: Throwable): Boolean = t match {
    case t: RemoteTransportException ⇒ isIndexMissing(t.getCause)
    case _: IndexNotFoundException   ⇒ true
    case _                           ⇒ false
  }
}