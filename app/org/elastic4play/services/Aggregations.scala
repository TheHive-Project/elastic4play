package org.elastic4play.services

import scala.util.Try
import scala.collection.JavaConverters._

import play.api.libs.json.{ JsArray, JsNumber, JsObject }

import com.sksamuel.elastic4s.ElasticDsl.{ avgAggregation, dateHistogramAggregation, filterAggregation, matchAllQuery, maxAggregation, minAggregation, nestedAggregation, sumAggregation, termsAggregation, topHitsAggregation }
import com.sksamuel.elastic4s.script.ScriptDefinition
import com.sksamuel.elastic4s.searches.RichSearchHit
import com.sksamuel.elastic4s.searches.aggs._
import org.elasticsearch.search.aggregations.bucket.filter.Filter
import org.elasticsearch.search.aggregations.bucket.filters.Filters
import org.elasticsearch.search.aggregations.bucket.histogram.{ DateHistogramInterval, Histogram }
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order
import org.elasticsearch.search.aggregations.bucket.nested.Nested
import org.elasticsearch.search.aggregations.metrics.avg.Avg
import org.elasticsearch.search.aggregations.metrics.max.Max
import org.elasticsearch.search.aggregations.metrics.min.Min
import org.elasticsearch.search.aggregations.metrics.sum.Sum
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits
import org.joda.time.DateTime

import org.elastic4play.BadRequestError
import org.elastic4play.database.DBUtils
import org.elastic4play.models.BaseModelDef

abstract class Agg(val aggregationName: String) {
  def apply(model: BaseModelDef): Seq[AggregationDefinition]

  def processResult(model: BaseModelDef, aggregations: RichAggregations): JsObject
}

abstract class FieldAgg(val fieldName: String, aggregationName: String, query: Option[QueryDef]) extends Agg(aggregationName) {
  def script(s: String): AggregationDefinition

  def field(f: String): AggregationDefinition

  def getAggregation(fieldName: String, aggregations: RichAggregations, query: Option[QueryDef]): RichAggregations = {

    val agg = query match {
      case None ⇒ aggregations
      case _    ⇒ RichAggregations(aggregations.aggregations.get[Filter](aggregationName).getAggregations)
    }

    if (fieldName.startsWith("computed")) agg
    else {
      fieldName.split("\\.").init.foldLeft(agg) { (a, _) ⇒
        RichAggregations(a.getAs[Nested](aggregationName).getAggregations)
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
        Seq(fieldName.split("\\.").init.foldLeft(field(fieldName)) { (agg, f) ⇒
          nestedAggregation(aggregationName, f).subaggs(agg)
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

  def processResult(model: BaseModelDef, aggregations: RichAggregations): JsObject = {
    val avg = getAggregation(fieldName, aggregations, query).getAs[Avg](aggregationName)
    val value = Try(JsNumber(avg.getValue)).toOption.getOrElse(JsNumber(0))
    JsObject(Seq(avg.getName → value))
  }
}

class SelectMin(aggregationName: String, fieldName: String, query: Option[QueryDef]) extends FieldAgg(fieldName, aggregationName, query) {
  def script(s: String): AggregationDefinition = minAggregation(aggregationName).script(ScriptDefinition(s).lang("groovy"))

  def field(f: String): AggregationDefinition = minAggregation(aggregationName).field(f)

  def processResult(model: BaseModelDef, aggregations: RichAggregations): JsObject = {
    val min = getAggregation(fieldName, aggregations, query).getAs[Min](aggregationName)
    val value = Try(JsNumber(min.getValue)).toOption.getOrElse(JsNumber(0))
    JsObject(Seq(min.getName → value))
  }
}

class SelectMax(aggregationName: String, fieldName: String, query: Option[QueryDef]) extends FieldAgg(fieldName, aggregationName, query) {
  def script(s: String): AggregationDefinition = maxAggregation(aggregationName).script(ScriptDefinition(s).lang("groovy"))

  def field(f: String): AggregationDefinition = maxAggregation(aggregationName).field(f)

  def processResult(model: BaseModelDef, aggregations: RichAggregations): JsObject = {
    val max = getAggregation(fieldName, aggregations, query).getAs[Max](aggregationName)
    val value = Try(JsNumber(max.getValue)).toOption.getOrElse(JsNumber(0))
    JsObject(Seq(max.getName → value))
  }
}

class SelectSum(aggregationName: String, fieldName: String, query: Option[QueryDef]) extends FieldAgg(fieldName, aggregationName, query) {
  def script(s: String): AggregationDefinition = sumAggregation(aggregationName).script(ScriptDefinition(s).lang("groovy"))

  def field(f: String): AggregationDefinition = sumAggregation(aggregationName).field(f)

  def processResult(model: BaseModelDef, aggregations: RichAggregations): JsObject = {
    val sum = getAggregation(fieldName, aggregations, query).getAs[Sum](aggregationName)
    val value = Try(JsNumber(sum.getValue)).toOption.getOrElse(JsNumber(0))
    JsObject(Seq(sum.getName → value))
  }
}

class SelectCount(aggregationName: String, query: Option[QueryDef]) extends FieldAgg("", aggregationName, query) {
  def script(s: String): AggregationDefinition = ???

  def field(f: String): AggregationDefinition = filterAggregation(aggregationName).query(matchAllQuery)

  def processResult(model: BaseModelDef, aggregations: RichAggregations): JsObject = {
    val count = aggregations.getAs[Filter](aggregationName)
    JsObject(Seq(count.getName → JsNumber(count.getDocCount)))
  }
}

class SelectTop(aggregationName: String, size: Int, sortBy: Seq[String], query: Option[QueryDef] = None) extends FieldAgg("", aggregationName, query) {
  def script(s: String): AggregationDefinition = ???

  def field(f: String): AggregationDefinition = topHitsAggregation(aggregationName).size(size).sortBy(DBUtils.sortDefinition(sortBy))

  def processResult(model: BaseModelDef, aggregations: RichAggregations): JsObject = {
    val top = aggregations.getAs[TopHits](aggregationName)
    JsObject(Seq("top" → JsArray(top.getHits.getHits.map(h ⇒ DBUtils.hit2json(RichSearchHit(h))))))
  }
}

class GroupByCategory(aggregationName: String, categories: Map[String, QueryDef], subAggs: Seq[Agg]) extends Agg(aggregationName) {
  def apply(model: BaseModelDef): Seq[KeyedFiltersAggregationDefinition] = {
    val filters = categories.mapValues(_.query)
    val subAggregations = subAggs.flatMap(_.apply(model))
    Seq(KeyedFiltersAggregationDefinition(aggregationName, filters).subAggregations(subAggregations))
  }

  def processResult(model: BaseModelDef, aggregations: RichAggregations): JsObject = {
    val filters = aggregations.getAs[Filters](aggregationName)
    JsObject {
      categories.keys.toSeq.map { cat ⇒
        val subAggResults = filters.getBucketByKey(cat).getAggregations
        cat → subAggs.map(_.processResult(model, RichAggregations(subAggResults)))
          .reduceOption(_ ++ _)
          .getOrElse(JsObject.empty)
      }
    }
  }
}

class GroupByTime(aggregationName: String, fields: Seq[String], interval: String, subAggs: Seq[Agg]) extends Agg(aggregationName) {
  def apply(model: BaseModelDef): Seq[DateHistogramAggregation] = {
    fields.map { f ⇒
      dateHistogramAggregation(s"${aggregationName}_$f").field(f).interval(new DateHistogramInterval(interval)).subAggregations(subAggs.flatMap(_.apply(model)))
    }
  }

  def processResult(model: BaseModelDef, aggregations: RichAggregations): JsObject = {
    val aggs = fields.map { f ⇒
      val buckets = aggregations.getAs[Histogram](s"${aggregationName}_$f").getBuckets
      f → buckets.asScala.map { bucket ⇒
        val results = subAggs
          .map(_.processResult(model, RichAggregations(bucket.getAggregations)))
          .reduceOption(_ ++ _)
          .getOrElse(JsObject.empty)
        // date -> obj(key{avg, min} -> value)
        bucket.getKey.asInstanceOf[DateTime].getMillis.toString → results
      }.toMap
    }.toMap
    val keys = aggs.values.flatMap(_.keys).toSet
    JsObject {
      keys.map { date ⇒
        date → JsObject(aggs.map {
          case (df, values) ⇒
            df → values.getOrElse(date, JsObject.empty)
        })
      }.toMap
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
        case "_count" | "+_count"   ⇒ Seq(Order.count(true))
        case "-_count"              ⇒ Seq(Order.count(false))
        case "_term" | "+_term"     ⇒ Seq(Order.term(true))
        case "-_term"               ⇒ Seq(Order.term(false))
        case f if f.startsWith("+") ⇒ Seq(Order.aggregation(f.drop(1), true))
        case f if f.startsWith("-") ⇒ Seq(Order.aggregation(f.drop(1), false))
        case f if f.length() > 0    ⇒ Seq(Order.aggregation(f, true))
        case _                      ⇒ Nil
      }
    if (sortDefinition.nonEmpty)
      agg.order(Order.compound(sortDefinition.asJava))
    else
      agg
  }

  def apply(model: BaseModelDef): Seq[AggregationDefinition] = {
    val agg = setSize(setOrder(termsAggregation(s"${aggregationName}_$fieldName").field(fieldName).subAggregations(subAggs.flatMap(_.apply(model)))))
    Seq(fieldName.split("\\.").init.foldLeft[AggregationDefinition](agg) { (agg, f) ⇒
      nestedAggregation(aggregationName, f).subaggs(agg)
    })
  }

  def processResult(model: BaseModelDef, aggregations: RichAggregations): JsObject = {
    val buckets = fieldName.split("\\.").init.foldLeft(aggregations) { (a, _) ⇒
      RichAggregations(a.getAs[Nested](aggregationName).getAggregations)
    }
      .getAs[Terms](s"${aggregationName}_$fieldName").getBuckets
    JsObject {
      buckets.asScala.map { bucket ⇒
        val results = subAggs
          .map(_.processResult(model, RichAggregations(bucket.getAggregations)))
          .reduceOption(_ ++ _)
          .getOrElse(JsObject.empty)
        bucket.getKeyAsString → results
      }.toMap
    }
  }
}
