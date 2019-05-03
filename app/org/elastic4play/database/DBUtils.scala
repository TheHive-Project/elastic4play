package org.elastic4play.database

import scala.annotation.tailrec

import play.api.libs.json._

import com.sksamuel.elastic4s.ElasticDsl.fieldSort
import com.sksamuel.elastic4s.searches.RichSearchHit
import com.sksamuel.elastic4s.searches.sort.FieldSortDefinition
import org.elasticsearch.index.IndexNotFoundException
import org.elasticsearch.transport.RemoteTransportException
import org.elasticsearch.search.sort.SortOrder.{ASC, DESC}

import org.elastic4play.utils

object DBUtils {

  def sortDefinition(sortBy: Seq[String]): Seq[FieldSortDefinition] = {
    val byFieldList: Seq[(String, FieldSortDefinition)] = sortBy
      .map {
        case f if f.startsWith("+") ⇒ f.drop(1) → fieldSort(f.drop(1)).order(ASC)
        case f if f.startsWith("-") ⇒ f.drop(1) → fieldSort(f.drop(1)).order(DESC)
        case f if f.length() > 0    ⇒ f         → fieldSort(f)
      }
    // then remove duplicates
    // Same as : val fieldSortDefs = byFieldList.groupBy(_._1).map(_._2.head).values.toSeq
    utils
      .Collection
      .distinctBy(byFieldList)(_._1)
      .map(_._2) :+ fieldSort("_uid").order(DESC)
  }

  /**
    * Transform search hit into JsObject
    * This function parses hit source add _type, _routing, _parent, _id and _version attributes
    */
  def hit2json(hit: RichSearchHit) = {
    val id = JsString(hit.id)
    Json.parse(hit.sourceAsString).as[JsObject] +
      ("_type"    → JsString(hit.`type`)) +
      ("_routing" → hit.fields.get("_routing").map(r ⇒ JsString(r.java.getValue[String])).getOrElse(id)) +
      ("_parent"  → hit.fields.get("_parent").map(r ⇒ JsString(r.java.getValue[String])).getOrElse(JsNull)) +
      ("_id"      → id) +
      ("_version" → JsNumber(hit.version))
  }

  @tailrec
  def isIndexMissing(t: Throwable): Boolean = t match {
    case t: RemoteTransportException ⇒ isIndexMissing(t.getCause)
    case _: IndexNotFoundException   ⇒ true
    case _                           ⇒ false
  }
}
