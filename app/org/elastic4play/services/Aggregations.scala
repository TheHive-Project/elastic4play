package org.elastic4play.services

import scala.util.Try
import scala.collection.JavaConverters._

import play.api.libs.json.{ JsArray, JsNumber, JsObject }

import com.sksamuel.elastic4s.ElasticDsl.{ avgAggregation, dateHistogramAggregation, filterAggregation, matchAllQuery, maxAggregation, minAggregation, nestedAggregation, sumAggregation, termsAggregation, topHitsAggregation }
import com.sksamuel.elastic4s.searches.RichSearchHit
import com.sksamuel.elastic4s.searches.aggs._
import com.sksamuel.elastic4s.searches.queries.QueryDefinition
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
import org.elastic4play.utils.Date.RichJoda

abstract class Agg(val aggregationName: String) {
  def apply(model: BaseModelDef): Seq[AggregationDefinition]

  def processResult(model: BaseModelDef, aggregations: RichAggregations): JsObject
}

object Agg {
  def filteredAgg(agg: AggregationDefinition, query: Option[QueryDef]): AggregationDefinition = {
    query match {
      case None    ⇒ agg
      case Some(q) ⇒ filterAggregation(agg.name).query(q.query).subAggregations(agg)
    }
  }
  def filteredResult(aggregations: RichAggregations, query: Option[QueryDef]): RichAggregations = {
    query.fold(aggregations)(_ ⇒ RichAggregations(aggregations.aggregations.iterator().next().asInstanceOf[Filter].getAggregations))
  }
}

abstract class FieldAgg(val fieldName: String, aggregationName: String) extends Agg(aggregationName) {
  def script(s: String): AggregationDefinition

  def field(f: String): AggregationDefinition

  def nested(fieldName: String, aggregations: Seq[AggregationDefinition]): Seq[AggregationDefinition] = {
    if (fieldName.startsWith("computed")) aggregations
    else {
      fieldName.split("\\.").init.foldLeft(aggregations) { (agg, f) ⇒
        Seq(nestedAggregation(aggregationName, f).subaggs(agg))
      }
    }
  }

  def getAggregation(fieldName: String, aggregations: RichAggregations, query: Option[QueryDef]): RichAggregations = {
    val filteredAggResult = Agg.filteredResult(aggregations, query)
    if (fieldName.startsWith("computed")) filteredAggResult
    else {
      fieldName.split("\\.").init.foldLeft(filteredAggResult) { (agg, _) ⇒
        RichAggregations(agg.getAs[Nested](aggregationName).getAggregations)
      }
    }
  }

  def apply(model: BaseModelDef): Seq[AggregationDefinition] = {
    fieldName.split("\\.") match {
      case Array("computed", c) ⇒
        val s = model.computedMetrics.getOrElse(
          c,
          throw BadRequestError(s"Field $fieldName is unknown in ${model.name}"))
        Seq(script(s))
      case array ⇒
        if (!model.attributes.exists(_.name == array(0))) {
          throw BadRequestError(s"Field $fieldName is unknown in ${model.name}")
        }
        // TODO check attribute type
        nested(fieldName, Seq(field(fieldName)))
    }
  }
}

class SelectAvg(aggregationName: String, fieldName: String, query: Option[QueryDef]) extends FieldAgg(fieldName, aggregationName) {
  def script(s: String): AggregationDefinition = Agg.filteredAgg(avgAggregation(aggregationName).script(s), query)

  def field(f: String): AggregationDefinition = Agg.filteredAgg(avgAggregation(aggregationName).field(f), query)

  def processResult(model: BaseModelDef, aggregations: RichAggregations): JsObject = {
    val avg = getAggregation(fieldName, aggregations, query).getAs[Avg](aggregationName)
    val value = Try(JsNumber(avg.getValue)).toOption.getOrElse(JsNumber(0))
    JsObject(Seq(avg.getName → value))
  }
}

class SelectMin(aggregationName: String, fieldName: String, query: Option[QueryDef]) extends FieldAgg(fieldName, aggregationName) {
  def script(s: String): AggregationDefinition = Agg.filteredAgg(minAggregation(aggregationName).script(s), query)

  def field(f: String): AggregationDefinition = Agg.filteredAgg(minAggregation(aggregationName).field(f), query)

  def processResult(model: BaseModelDef, aggregations: RichAggregations): JsObject = {
    val min = getAggregation(fieldName, aggregations, query).getAs[Min](aggregationName)
    val value = Try(JsNumber(min.getValue)).toOption.getOrElse(JsNumber(0))
    JsObject(Seq(min.getName → value))
  }
}

class SelectMax(aggregationName: String, fieldName: String, query: Option[QueryDef]) extends FieldAgg(fieldName, aggregationName) {
  def script(s: String): AggregationDefinition = Agg.filteredAgg(maxAggregation(aggregationName).script(s), query)

  def field(f: String): AggregationDefinition = Agg.filteredAgg(maxAggregation(aggregationName).field(f), query)

  def processResult(model: BaseModelDef, aggregations: RichAggregations): JsObject = {
    val max = getAggregation(fieldName, aggregations, query).getAs[Max](aggregationName)
    val value = Try(JsNumber(max.getValue)).toOption.getOrElse(JsNumber(0))
    JsObject(Seq(max.getName → value))
  }
}

class SelectSum(aggregationName: String, fieldName: String, query: Option[QueryDef]) extends FieldAgg(fieldName, aggregationName) {
  def script(s: String): AggregationDefinition = Agg.filteredAgg(sumAggregation(aggregationName).script(s), query)

  def field(f: String): AggregationDefinition = Agg.filteredAgg(sumAggregation(aggregationName).field(f), query)

  def processResult(model: BaseModelDef, aggregations: RichAggregations): JsObject = {
    val sum = getAggregation(fieldName, aggregations, query).getAs[Sum](aggregationName)
    val value = Try(JsNumber(sum.getValue)).toOption.getOrElse(JsNumber(0))
    JsObject(Seq(sum.getName → value))
  }
}

class SelectCount(aggregationName: String, query: Option[QueryDef]) extends Agg(aggregationName) {
  override def apply(model: BaseModelDef) = Seq(filterAggregation(aggregationName).query(query.fold[QueryDefinition](matchAllQuery)(_.query)))

  def processResult(model: BaseModelDef, aggregations: RichAggregations): JsObject = {
    val count = aggregations.getAs[Filter](aggregationName)
    JsObject(Seq(count.getName → JsNumber(count.getDocCount)))
  }
}

class SelectTop(aggregationName: String, size: Int, sortBy: Seq[String], query: Option[QueryDef] = None) extends Agg(aggregationName) {
  def apply(model: BaseModelDef) = Seq(Agg.filteredAgg(topHitsAggregation(aggregationName).size(size).sortBy(DBUtils.sortDefinition(sortBy)), query))

  def processResult(model: BaseModelDef, aggregations: RichAggregations): JsObject = {
    val top = Agg.filteredResult(aggregations, query)
      .getAs[TopHits](aggregationName)
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
          .getOrElse(JsObject(Nil))
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
          .getOrElse(JsObject(Nil))
        // date -> obj(key{avg, min} -> value)
        bucket.getKey.asInstanceOf[DateTime].toIso → results
      }.toMap
    }.toMap
    val keys = aggs.values.flatMap(_.keys).toSet
    JsObject {
      keys.map { date ⇒
        date → JsObject(aggs.map {
          case (df, values) ⇒
            df → values.getOrElse(date, JsObject(Nil))
        })
      }.toMap
    }
  }
}

class GroupByField(aggregationName: String, field: String, size: Option[Int], sortBy: Seq[String], subAggs: Seq[Agg]) extends Agg(aggregationName) {
  def apply(model: BaseModelDef): Seq[TermsAggregationDefinition] = {
    Seq(termsAggregation(s"${aggregationName}_$field").field(field).subAggregations(subAggs.flatMap(_.apply(model))))
      .map { agg ⇒ size.fold(agg)(s ⇒ agg.size(s)) }
      .map {
        case agg if sortBy.isEmpty ⇒ agg
        case agg ⇒
          val sortDefinition = sortBy
            .flatMap {
              case "_count" | "+_count"   ⇒ Seq(Order.count(true))
              case "-_count"              ⇒ Seq(Order.count(false))
              case "_term" | "+_term"     ⇒ Seq(Order.term(true))
              case "-_term"               ⇒ Seq(Order.term(false))
              case f if f.startsWith("+") ⇒ Seq(Order.aggregation(f.drop(1), true))
              case f if f.startsWith("-") ⇒ Seq(Order.aggregation(f.drop(1), false))
              case f if f.length() > 0    ⇒ Seq(Order.aggregation(f, true))
            }
          agg.order(Order.compound(sortDefinition.asJava))
      }
  }

  def processResult(model: BaseModelDef, aggregations: RichAggregations): JsObject = {
    val buckets = aggregations.getAs[Terms](s"${aggregationName}_$field").getBuckets
    JsObject {
      buckets.asScala.map { bucket ⇒
        val results = subAggs
          .map(_.processResult(model, RichAggregations(bucket.getAggregations)))
          .reduceOption(_ ++ _)
          .getOrElse(JsObject(Nil))
        bucket.getKeyAsString → results
      }.toMap
    }
  }
}
