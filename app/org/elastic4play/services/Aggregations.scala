package org.elastic4play.services

import play.api.libs.json._

import com.sksamuel.elastic4s.http.ElasticDsl.{ avgAggregation, dateHistogramAggregation, filterAggregation, matchAllQuery, maxAggregation, minAggregation, nestedAggregation, sumAggregation, termsAggregation, topHitsAggregation }
import com.sksamuel.elastic4s.http.search.HasAggregations
import com.sksamuel.elastic4s.script.ScriptDefinition
import com.sksamuel.elastic4s.searches.DateHistogramInterval
import com.sksamuel.elastic4s.searches.aggs._

import org.elastic4play.BadRequestError
import org.elastic4play.database.DBUtils
import org.elastic4play.models.BaseModelDef

//case class Aggregations(data: Map[String, Any]) {
//  def apply(name: String): Aggregations = Aggregations(data(name).asInstanceOf[Map[String, Any]])
//  def get[T](name: String): T = data(name).asInstanceOf[T]
//}

abstract class Agg(val aggregationName: String) {
  def apply(model: BaseModelDef): Seq[AggregationDefinition]

  def processResult(model: BaseModelDef, aggregations: HasAggregations): JsObject
}

abstract class FieldAgg(val fieldName: String, aggregationName: String, query: Option[QueryDef]) extends Agg(aggregationName) {
  def script(s: String): AggregationDefinition

  def field(f: String): AggregationDefinition

  def getAggregation(aggregations: HasAggregations): HasAggregations = {
    val filteredAgg: HasAggregations = query match {
      case None ⇒ aggregations
      case _    ⇒ aggregations.filter(aggregationName)
    }

    if (fieldName.startsWith("computed")) filteredAgg
    else {
      fieldName.split("\\.").init.foldLeft(filteredAgg) { (a, _) ⇒
        a.nested(aggregationName)
      }
    }
  }

  def apply(model: BaseModelDef): Seq[AggregationDefinition] = {
    val aggs = fieldName.split("\\.") match {
      case Array("computed", c) ⇒
        val s = model.computedMetrics.getOrElse(
          c,
          throw BadRequestError(s"Field $fieldName is unknown in ${model.modelName}"))
        Seq(script(s))
      case array ⇒
        if (array(0) != "" && !model.attributes.exists(_.attributeName == array(0))) {
          throw BadRequestError(s"Field $fieldName is unknown in ${model.modelName}")
        }
        // TODO check attribute type
        Seq(fieldName
          .split("\\.")
          .toSeq
          .init
          .inits
          .toSeq
          .init
          .foldLeft[AggregationDefinition](field(fieldName)) { (agg, f) ⇒
            nestedAggregation(aggregationName, f.mkString(".")).subaggs(agg)
          })
    }
    query match {
      case None    ⇒ aggs
      case Some(q) ⇒ Seq(filterAggregation(aggregationName).query(q.query).subAggregations(aggs))
    }
  }
}

class SelectAvg(aggregationName: String, fieldName: String, query: Option[QueryDef]) extends FieldAgg(fieldName, aggregationName, query) {
  def script(s: String): AggregationDefinition = avgAggregation(aggregationName).script(ScriptDefinition(s).lang("groovy"))

  def field(f: String): AggregationDefinition = avgAggregation(aggregationName).field(f)

  def processResult(model: BaseModelDef, aggregations: HasAggregations): JsObject = {
    Json.obj(aggregationName -> aggregations.avg(aggregationName).value)
  }
}

class SelectMin(aggregationName: String, fieldName: String, query: Option[QueryDef]) extends FieldAgg(fieldName, aggregationName, query) {
  def script(s: String): AggregationDefinition = minAggregation(aggregationName).script(ScriptDefinition(s).lang("groovy"))

  def field(f: String): AggregationDefinition = minAggregation(aggregationName).field(f)

  def processResult(model: BaseModelDef, aggregations: HasAggregations): JsObject = {
    Json.obj(aggregationName -> aggregations.min(aggregationName).value)
  }
}

class SelectMax(aggregationName: String, fieldName: String, query: Option[QueryDef]) extends FieldAgg(fieldName, aggregationName, query) {
  def script(s: String): AggregationDefinition = maxAggregation(aggregationName).script(ScriptDefinition(s).lang("groovy"))

  def field(f: String): AggregationDefinition = maxAggregation(aggregationName).field(f)

  def processResult(model: BaseModelDef, aggregations: HasAggregations): JsObject = {
    Json.obj(aggregationName -> aggregations.max(aggregationName).value)
  }
}

class SelectSum(aggregationName: String, fieldName: String, query: Option[QueryDef]) extends FieldAgg(fieldName, aggregationName, query) {
  def script(s: String): AggregationDefinition = sumAggregation(aggregationName).script(ScriptDefinition(s).lang("groovy"))

  def field(f: String): AggregationDefinition = sumAggregation(aggregationName).field(f)

  def processResult(model: BaseModelDef, aggregations: HasAggregations): JsObject = {
    Json.obj(aggregationName -> aggregations.sum(aggregationName).value)
  }
}

class SelectCount(aggregationName: String, query: Option[QueryDef]) extends FieldAgg("", aggregationName, query) {
  def script(s: String): AggregationDefinition = ???

  def field(f: String): AggregationDefinition = filterAggregation(aggregationName).query(matchAllQuery)

  def processResult(model: BaseModelDef, aggregations: HasAggregations): JsObject = {
    Json.obj(aggregationName -> aggregations.filter(aggregationName).docCount)
  }
}

class SelectTop(aggregationName: String, size: Int, sortBy: Seq[String], query: Option[QueryDef] = None) extends FieldAgg("", aggregationName, query) {
  def script(s: String): AggregationDefinition = ???

  def field(f: String): AggregationDefinition = topHitsAggregation(aggregationName).size(size).sortBy(DBUtils.sortDefinition(sortBy))

  private def toJson(value: Any): JsValue = value match {
    case obj: Map[_, _] ⇒ JsObject(obj.map(kv ⇒ kv._1.toString -> toJson(kv._2)))
  }

  def processResult(model: BaseModelDef, aggregations: HasAggregations): JsObject = {
    val topHits = aggregations.tophits(aggregationName).hits.map { h ⇒
      toJson(h.source).as[JsObject] +
        ("_type" -> JsString(h.`type`)) +
        ("_id" -> JsString(h.id))
    }
    Json.obj("top" -> topHits)
  }
}

class GroupByCategory(aggregationName: String, categories: Map[String, QueryDef], subAggs: Seq[Agg]) extends Agg(aggregationName) {
  def apply(model: BaseModelDef): Seq[KeyedFiltersAggregationDefinition] = {
    val filters = categories.mapValues(_.query)
    val subAggregations = subAggs.flatMap(_.apply(model))
    Seq(KeyedFiltersAggregationDefinition(aggregationName, filters).subAggregations(subAggregations))
  }

  def processResult(model: BaseModelDef, aggregations: HasAggregations): JsObject = {
    JsObject {
      aggregations.keyedFilters(aggregationName)
        .aggResults
        .mapValues { value ⇒
          subAggs.map(_.processResult(model, value))
            .fold(JsObject.empty)(_ ++ _)
        }
    }
  }
}

class GroupByTime(aggregationName: String, fields: Seq[String], interval: String, subAggs: Seq[Agg]) extends Agg(aggregationName) {
  def apply(model: BaseModelDef): Seq[DateHistogramAggregation] = {
    fields.map { f ⇒
      dateHistogramAggregation(s"${aggregationName}_$f").field(f).interval(DateHistogramInterval.fromString(interval)).subAggregations(subAggs.flatMap(_.apply(model)))
    }
  }

  def processResult(model: BaseModelDef, aggregations: HasAggregations): JsObject = {
    JsObject {
      aggregations.dateHistogram(aggregationName)
        .buckets
        .map { b ⇒
          b.date -> subAggs.map(_.processResult(model, b))
            .fold(JsObject.empty)(_ ++ _)
        }
    }
  }
}

class GroupByField(aggregationName: String, fieldName: String, size: Option[Int], sortBy: Seq[String], subAggs: Seq[Agg]) extends Agg(aggregationName) {
  private def setSize(agg: TermsAggregationDefinition): TermsAggregationDefinition = {
    size.fold(agg)(s ⇒ agg.size(s))
  }

  private def setOrder(agg: TermsAggregationDefinition): TermsAggregationDefinition = {
    val sortDefinition = sortBy
      .flatMap {
        case f if f.startsWith("+") ⇒ Seq(TermsOrder(f.drop(1), asc = true))
        case f if f.startsWith("-") ⇒ Seq(TermsOrder(f.drop(1), asc = false))
        case f if f.length() > 0    ⇒ Seq(TermsOrder(f, asc = true))
        case _                      ⇒ Nil
      }
    if (sortDefinition.nonEmpty)
      agg.order(sortDefinition)
    else
      agg
  }

  def apply(model: BaseModelDef): Seq[AggregationDefinition] = {
    val agg = setSize(setOrder(termsAggregation(s"${aggregationName}_$fieldName").field(fieldName).subAggregations(subAggs.flatMap(_.apply(model)))))
    Seq(fieldName
      .split("\\.")
      .toSeq
      .init
      .inits
      .toSeq
      .init
      .foldLeft[AggregationDefinition](agg) { (agg, f) ⇒
        nestedAggregation(aggregationName, f.mkString(".")).subaggs(agg)
      })
  }

  def processResult(model: BaseModelDef, aggregations: HasAggregations): JsObject = {
    JsObject {
      aggregations.terms(aggregationName).buckets
        .map { b ⇒
          b.key -> subAggs.map(_.processResult(model, b))
            .fold(JsObject.empty)(_ ++ _)
        }
    }
  }
}