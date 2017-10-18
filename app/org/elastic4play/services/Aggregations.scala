package org.elastic4play.services

import scala.util.Try
import scala.collection.JavaConverters._

import play.api.libs.json.{ JsArray, JsNumber, JsObject }

import com.sksamuel.elastic4s.ElasticDsl.{ avgAggregation, dateHistogramAggregation, filterAggregation, matchAllQuery, maxAggregation, minAggregation, nestedAggregation, sumAggregation, termsAggregation, topHitsAggregation }
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
import org.elastic4play.utils.Date.RichJoda

trait Agg {
  def apply(model: BaseModelDef): Seq[AggregationDefinition]
  def processResult(model: BaseModelDef, aggregations: RichAggregations): JsObject
}

trait FieldSelectable { self: Agg ⇒
  val aggregationName: String
  val fieldName: String
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
  def getAggregation(fieldName: String, aggregations: RichAggregations): RichAggregations = {
    if (fieldName.startsWith("computed")) aggregations
    else {
      fieldName.split("\\.").init.foldLeft(aggregations) { (agg, _) ⇒
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

class SelectAvg(val fieldName: String) extends Agg with FieldSelectable {
  val aggregationName = s"avg_$fieldName"
  def script(s: String): AvgAggregationDefinition = avgAggregation(aggregationName).script(s)
  def field(f: String): AvgAggregationDefinition = avgAggregation(aggregationName).field(f)
  def processResult(model: BaseModelDef, aggregations: RichAggregations): JsObject = {
    val avg = getAggregation(fieldName, aggregations).getAs[Avg](aggregationName)
    val value = Try(JsNumber(avg.getValue)).toOption.getOrElse(JsNumber(0))
    JsObject(Seq(avg.getName → value))
  }
}

class SelectMin(val fieldName: String) extends Agg with FieldSelectable {
  val aggregationName = s"min_$fieldName"
  def script(s: String): MinAggregationDefinition = minAggregation(aggregationName).script(s)
  def field(f: String): MinAggregationDefinition = minAggregation(aggregationName).field(f)
  def processResult(model: BaseModelDef, aggregations: RichAggregations): JsObject = {
    val min = getAggregation(fieldName, aggregations).getAs[Min](aggregationName)
    val value = Try(JsNumber(min.getValue)).toOption.getOrElse(JsNumber(0))
    JsObject(Seq(min.getName → value))
  }
}

class SelectMax(val fieldName: String) extends Agg with FieldSelectable {
  val aggregationName = s"max_$fieldName"
  def script(s: String): MaxAggregationDefinition = maxAggregation(aggregationName).script(s)
  def field(f: String): MaxAggregationDefinition = maxAggregation(aggregationName).field(f)
  def processResult(model: BaseModelDef, aggregations: RichAggregations): JsObject = {
    val max = getAggregation(fieldName, aggregations).getAs[Max](aggregationName)
    val value = Try(JsNumber(max.getValue)).toOption.getOrElse(JsNumber(0))
    JsObject(Seq(max.getName → value))
  }
}

class SelectSum(val fieldName: String) extends Agg with FieldSelectable {
  val aggregationName = s"sum_$fieldName"
  def script(s: String): SumAggregationDefinition = sumAggregation(aggregationName).script(s)
  def field(f: String): SumAggregationDefinition = sumAggregation(aggregationName).field(f)
  def processResult(model: BaseModelDef, aggregations: RichAggregations): JsObject = {
    val sum = getAggregation(fieldName, aggregations).getAs[Sum](aggregationName)
    val value = Try(JsNumber(sum.getValue)).toOption.getOrElse(JsNumber(0))
    JsObject(Seq(sum.getName → value))
  }
}

object SelectCount extends Agg {
  val name = "count"
  override def apply(model: BaseModelDef) = Seq(filterAggregation(name).query(matchAllQuery))
  def processResult(model: BaseModelDef, aggregations: RichAggregations): JsObject = {
    val count = aggregations.getAs[Filter](name)
    JsObject(Seq(count.getName → JsNumber(count.getDocCount)))
  }
}

class SelectTop(size: Int, sortBy: Seq[String]) extends Agg {
  val name = "top"
  def apply(model: BaseModelDef) = Seq(topHitsAggregation(name).size(size).sortBy(DBUtils.sortDefinition(sortBy)))
  def processResult(model: BaseModelDef, aggregations: RichAggregations): JsObject = {
    val top = aggregations.getAs[TopHits](name)
    JsObject(Seq("top" → JsArray(top.getHits.getHits.map(h ⇒ DBUtils.hit2json(RichSearchHit(h))))))
  }
}

class GroupByCategory(categories: Map[String, QueryDef], subAggs: Seq[Agg]) extends Agg {
  val name = "categories"
  def apply(model: BaseModelDef): Seq[KeyedFiltersAggregationDefinition] = {
    val filters = categories.mapValues(_.query)
    val subAggregations = subAggs.flatMap(_.apply(model))
    Seq(KeyedFiltersAggregationDefinition(name, filters).subAggregations(subAggregations))
  }
  def processResult(model: BaseModelDef, aggregations: RichAggregations): JsObject = {
    val filters = aggregations.getAs[Filters](name)
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
class GroupByTime(fields: Seq[String], interval: String, subAggs: Seq[Agg]) extends Agg {
  def apply(model: BaseModelDef): Seq[DateHistogramAggregation] = {
    fields.map { f ⇒
      dateHistogramAggregation(s"datehistogram_$f").field(f).interval(new DateHistogramInterval(interval)).subAggregations(subAggs.flatMap(_.apply(model)))
    }
  }
  def processResult(model: BaseModelDef, aggregations: RichAggregations): JsObject = {
    val aggs = fields.map { f ⇒
      val buckets = aggregations.getAs[Histogram](s"datehistogram_$f").getBuckets
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
class GroupByField(field: String, size: Option[Int], sortBy: Seq[String], subAggs: Seq[Agg]) extends Agg {
  def apply(model: BaseModelDef): Seq[TermsAggregationDefinition] = {
    Seq(termsAggregation(s"term_$field").field(field).subAggregations(subAggs.flatMap(_.apply(model))))
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
    val buckets = aggregations.getAs[Terms](s"term_$field").getBuckets
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
