package org.elastic4play.database

import play.api.libs.json.{ JsNull, JsObject, JsString, Json }

import org.elasticsearch.index.IndexNotFoundException
import org.elasticsearch.transport.RemoteTransportException

import com.sksamuel.elastic4s.{ FieldSortDefinition, RichSearchHit }
import com.sksamuel.elastic4s.ElasticDsl.field

import org.elastic4play.models.Attribute
import org.elastic4play.utils

object DBUtils {
  def sortDefinition(sortBy: Seq[String]) = {
    import org.elasticsearch.search.sort.SortOrder._
    val byFieldList: Seq[(String, FieldSortDefinition)] = sortBy
      .map {
        case f if f.startsWith("+") => f.drop(1) -> (field sort f.drop(1) order ASC)
        case f if f.startsWith("-") => f.drop(1) -> (field sort f.drop(1) order DESC)
        case f if f.length() > 0    => f -> (field sort f)
      }
    // then remove duplicates
    // Same as : val fieldSortDefs = byFieldList.groupBy(_._1).map(_._2.head).values.toSeq
    utils.Collection
      .distinctBy(byFieldList)(_._1)
      .map(_._2) :+ (field sort "_uid" order DESC)
  }

  def hit2json(fields: Option[Seq[Attribute[_]]], hit: RichSearchHit): JsObject = {
    val fieldsValue = hit.fields
    val id = JsString(hit.id)
    Option(hit.sourceAsString).filterNot(_ == "").fold(JsObject(Nil))(s => Json.parse(s).as[JsObject]) ++
      fields.fold(JsObject(Nil)) { rf =>
        JsObject(rf flatMap { attr => fieldsValue.get(attr.name).flatMap(f => attr.format.elasticToJson(f.values.toSeq)).map(attr.name -> _) })
      } +
      ("_type" -> JsString(hit.`type`)) +
      ("_routing" -> fieldsValue.get("_routing").map(r => JsString(r.value[String])).getOrElse(id)) +
      ("_parent" -> fieldsValue.get("_parent").map(r => JsString(r.value[String])).getOrElse(JsNull)) +
      ("_id" -> id)
  }

  def isIndexMissing(t: Throwable): Boolean = t match {
    case t: RemoteTransportException => isIndexMissing(t.getCause)
    case _: IndexNotFoundException   => true
    case _                           => false
  }
}