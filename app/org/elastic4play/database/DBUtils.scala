package org.elastic4play.database

import play.api.libs.json._
import com.sksamuel.elastic4s.ElasticDsl.fieldSort
import com.sksamuel.elastic4s.requests.searches.SearchHit
import com.sksamuel.elastic4s.requests.searches.sort.{Sort, SortOrder}
import org.elastic4play.utils

object DBUtils {

  def sortDefinition(sortBy: Seq[String]): Seq[Sort] = {
    val byFieldList: Seq[(String, Sort)] = sortBy
      .map {
        case f if f.startsWith("+") => f.drop(1) -> fieldSort(f.drop(1)).order(SortOrder.ASC)
        case f if f.startsWith("-") => f.drop(1) -> fieldSort(f.drop(1)).order(SortOrder.DESC)
        case f if f.length() > 0    => f         -> fieldSort(f)
      }
    // then remove duplicates
    // Same as : val fieldSortDefs = byFieldList.groupBy(_._1).map(_._2.head).values.toSeq
    utils
      .Collection
      .distinctBy(byFieldList)(_._1)
      .map(_._2) :+ fieldSort("_doc").order(SortOrder.DESC)
  }

  /**
    * Transform search hit into JsObject
    * This function parses hit source add _type, _routing, _parent, _id, _seqNo and _primaryTerm  attributes
    */
  def hit2json(hit: SearchHit): JsObject = {
    val id   = JsString(hit.id)
    val body = Json.parse(hit.sourceAsString).as[JsObject]
    // Retrieve document type from the field "relations" for index pre-ES7.11
    // This method is used by migration. "relations" must be used as fall-back to permit migration from pre-ES7.11
    val model  = (body \ "docType").asOpt[JsString].orElse((body \ "relations" \ "name").asOpt[JsString]).getOrElse((body \ "relations").as[JsString])
    val parent = (body \ "relations" \ "parent").asOpt[JsString].getOrElse(JsNull)
    body - "relations" - "docType" +
      ("_type"        -> model) +
      ("_routing"     -> hit.routing.fold(id)(JsString.apply)) +
      ("_parent"      -> parent) +
      ("_id"          -> id) +
      ("_seqNo"       -> JsNumber(hit.seqNo)) +
      ("_primaryTerm" -> JsNumber(hit.primaryTerm))
  }
}
