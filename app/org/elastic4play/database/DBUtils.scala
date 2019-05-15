package org.elastic4play.database

import play.api.libs.json._

import com.sksamuel.elastic4s.http.ElasticDsl.fieldSort
import com.sksamuel.elastic4s.http.search.SearchHit
import com.sksamuel.elastic4s.searches.sort.Sort
import com.sksamuel.elastic4s.searches.sort.SortOrder.{ASC, DESC}

import org.elastic4play.utils

object DBUtils {

  def sortDefinition(sortBy: Seq[String]): Seq[Sort] = {
    val byFieldList: Seq[(String, Sort)] = sortBy
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
      .map(_._2) :+ fieldSort("_id").order(DESC)
  }

  /**
    * Transform search hit into JsObject
    * This function parses hit source add _type, _routing, _parent, _id and _version attributes
    */
  def hit2json(hit: SearchHit) = {
    val id   = JsString(hit.id)
    val body = Json.parse(hit.sourceAsString).as[JsObject]
    val (parent, model) = (body \ "relations" \ "parent").asOpt[JsString] match {
      case Some(p) ⇒ p      → (body \ "relations" \ "name").as[JsString]
      case None    ⇒ JsNull → (body \ "relations").as[JsString]
    }
    body - "relations" +
      ("_type"    → model) +
      ("_routing" → hit.routing.fold(id)(JsString.apply)) +
      ("_parent"  → parent) +
      ("_id"      → id) +
      ("_version" → JsNumber(hit.version))
  }
}
