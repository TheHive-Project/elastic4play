package org.elastic4play.services

import javax.inject.{ Inject, Singleton }

import scala.collection.JavaConverters._
import scala.concurrent.{ ExecutionContext, Future }
import scala.math.BigDecimal.{ double2bigDecimal, int2bigDecimal, long2bigDecimal }
import scala.util.Try

import play.api.libs.json.JsValue.jsValueToJsLookup
import play.api.libs.json.{ JsArray, JsNumber, JsObject }

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.searches.RichSearchHit
import com.sksamuel.elastic4s.searches.aggs.{ AggregationDefinition, KeyedFiltersAggregationDefinition, RichAggregations }
import com.sksamuel.elastic4s.searches.queries.{ BuildableTermsQueryImplicits, QueryDefinition }
import org.apache.lucene.search.join.ScoreMode
import org.elasticsearch.search.aggregations.bucket.filter.Filter
import org.elasticsearch.search.aggregations.bucket.filters.Filters
import org.elasticsearch.search.aggregations.bucket.histogram.{ DateHistogramInterval, Histogram }
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order
import org.elasticsearch.search.aggregations.metrics.avg.Avg
import org.elasticsearch.search.aggregations.metrics.max.Max
import org.elasticsearch.search.aggregations.metrics.min.Min
import org.elasticsearch.search.aggregations.metrics.sum.Sum
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits
import org.joda.time.DateTime

import org.elastic4play.BadRequestError
import org.elastic4play.database.{ DBConfiguration, DBFind, DBUtils }
import org.elastic4play.models.{ AbstractModelDef, BaseEntity, BaseModelDef }
import org.elastic4play.utils.Date.RichJoda

case class QueryDef(query: QueryDefinition)

trait Agg {
  def apply(model: BaseModelDef): Seq[AggregationDefinition]
  def processResult(model: BaseModelDef, aggregations: RichAggregations): JsObject
}

trait FieldSelectable { self: Agg ⇒
  val fieldName: String
  def script(s: String): AggregationDefinition
  def field(f: String): AggregationDefinition
  def apply(model: BaseModelDef) = {
    fieldName.split("\\.", 3) match {
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
        Seq(field(fieldName))
    }
  }
}

class SelectAvg(val fieldName: String) extends Agg with FieldSelectable {
  val name = s"avg_$fieldName"
  def script(s: String) = avgAggregation(name).script(s)
  def field(f: String) = avgAggregation(name).field(f)
  def processResult(model: BaseModelDef, aggregations: RichAggregations): JsObject = {
    val avg = aggregations.getAs[Avg](name)
    val value = Try(JsNumber(avg.getValue)).toOption.getOrElse(JsNumber(0))
    JsObject(Seq(avg.getName → value))
  }
}

class SelectMin(val fieldName: String) extends Agg with FieldSelectable {
  val name = s"min_$fieldName"
  def script(s: String) = minAggregation(name).script(s)
  def field(f: String) = minAggregation(name).field(f)
  def processResult(model: BaseModelDef, aggregations: RichAggregations): JsObject = {
    val min = aggregations.getAs[Min](name)
    val value = Try(JsNumber(min.getValue)).toOption.getOrElse(JsNumber(0))
    JsObject(Seq(min.getName → value))
  }
}

class SelectMax(val fieldName: String) extends Agg with FieldSelectable {
  val name = s"max_$fieldName"
  def script(s: String) = maxAggregation(name).script(s)
  def field(f: String) = maxAggregation(name).field(f)
  def processResult(model: BaseModelDef, aggregations: RichAggregations): JsObject = {
    val max = aggregations.getAs[Max](name)
    val value = Try(JsNumber(max.getValue)).toOption.getOrElse(JsNumber(0))
    JsObject(Seq(max.getName → value))
  }
}

class SelectSum(val fieldName: String) extends Agg with FieldSelectable {
  val name = s"sum_$fieldName"
  def script(s: String) = sumAggregation(name).script(s)
  def field(f: String) = sumAggregation(name).field(f)
  def processResult(model: BaseModelDef, aggregations: RichAggregations): JsObject = {
    val sum = aggregations.getAs[Sum](name)
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
  def apply(model: BaseModelDef) = {
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
  def apply(model: BaseModelDef) = {
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
  def apply(model: BaseModelDef) = {
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
object QueryDSL {
  def selectAvg(field: String) = new SelectAvg(field)
  def selectMin(field: String) = new SelectMin(field)
  def selectMax(field: String) = new SelectMax(field)
  def selectSum(field: String) = new SelectSum(field)
  val selectCount = SelectCount
  def selectTop(size: Int, sortBy: Seq[String]) = new SelectTop(size, sortBy)
  def groupByTime(fields: Seq[String], interval: String, selectables: Agg*) = new GroupByTime(fields, interval, selectables)
  def groupByField(field: String, selectables: Agg*) = new GroupByField(field, None, Nil, selectables)
  def groupByField(field: String, size: Int, sortBy: Seq[String], selectables: Agg*) = new GroupByField(field, Some(size), sortBy, selectables)
  def groupByCaterogy(categories: Map[String, QueryDef], selectables: Agg*) = new GroupByCategory(categories, selectables)

  private def nestedField(field: String, q: (String) ⇒ QueryDefinition) = {
    val names = field.split("\\.")
    names.init.foldRight(q(field)) {
      case (subName, queryDef) ⇒ nestedQuery(subName).query(queryDef).scoreMode(ScoreMode.None)
    }
  }

  implicit class SearchField(field: String) extends BuildableTermsQueryImplicits {
    private def convertValue(value: Any): Any = value match {
      case _: Enumeration#Value ⇒ value.toString
      case _                    ⇒ value
    }
    def ~=(value: Any) = QueryDef(nestedField(field, termQuery(_, convertValue(value))))
    def ~!=(value: Any) = not(QueryDef(nestedField(field, termQuery(_, convertValue(value)))))
    def ~<(value: Any) = QueryDef(nestedField(field, rangeQuery(_).lt(value.toString)))
    def ~>(value: Any) = QueryDef(nestedField(field, rangeQuery(_).gt(value.toString)))
    def ~<=(value: Any) = QueryDef(nestedField(field, rangeQuery(_).lte(value.toString)))
    def ~>=(value: Any) = QueryDef(nestedField(field, rangeQuery(_).gte(value.toString)))
    def ~<>(value: (Any, Any)) = QueryDef(nestedField(field, rangeQuery(_).gt(value._1.toString).lt(value._2.toString)))
    def ~=<>=(value: (Any, Any)) = QueryDef(nestedField(field, rangeQuery(_).gte(value._1.toString).lte(value._2.toString)))
    def in(values: AnyRef*) = QueryDef(nestedField(field, termsQuery(_, values)))
  }

  def ofType(value: String) = QueryDef(termQuery("_type", value))
  def withId(entityIds: String*): QueryDef = QueryDef(idsQuery(entityIds))
  def any: QueryDef = QueryDef(matchAllQuery)
  def contains(field: String): QueryDef = QueryDef(nestedField(field, existsQuery))
  def or(queries: QueryDef*): QueryDef = or(queries)
  def or(queries: Iterable[QueryDef]): QueryDef = QueryDef(boolQuery().should(queries.map(_.query)))
  def and(queries: QueryDef*): QueryDef = QueryDef(boolQuery().must(queries.map(_.query)))
  def and(queries: Iterable[QueryDef]): QueryDef = QueryDef(boolQuery().must(queries.map(_.query)))
  def not(query: QueryDef): QueryDef = QueryDef(boolQuery.not(query.query))
  def child(childType: String, query: QueryDef): QueryDef = QueryDef(hasChildQuery(childType).query(query.query).scoreMode(ScoreMode.None))
  def parent(parentType: String, query: QueryDef): QueryDef = QueryDef(hasParentQuery(parentType).query(query.query).scoreMode(false))
  def withParent(parent: BaseEntity): QueryDef = withParent(parent.model.name, parent.id)
  def withParent(parentType: String, parentId: String): QueryDef = QueryDef(hasParentQuery(parentType).query(idsQuery(parentId).types(parentType)).scoreMode(false)) // QueryDef(ParentIdQueryDefinition(parentType, parentId)) FIXME doesn't work yet
  def string(queryString: String): QueryDef = QueryDef(query(queryString))
}

@Singleton
class FindSrv @Inject() (
    dbfind: DBFind,
    modelSrv: ModelSrv,
    implicit val ec: ExecutionContext) {

  def switchTo(db: DBConfiguration) = new FindSrv(dbfind.switchTo(db), modelSrv, ec)

  def apply(modelName: Option[String], queryDef: QueryDef, range: Option[String], sortBy: Seq[String]): (Source[BaseEntity, NotUsed], Future[Long]) = {
    val (src, total) = dbfind(range, sortBy)(indexName ⇒ modelName.fold(search(indexName))(m ⇒ search(indexName → m)).query(queryDef.query))
    val entities = src.map { attrs ⇒
      modelName match {
        //case Some("audit") => auditModel.get()(attrs)
        case Some(m) ⇒ modelSrv(m).getOrElse(sys.error("TODO"))(attrs)
        case None ⇒
          val tpe = (attrs \ "_type").asOpt[String].getOrElse(sys.error("TODO"))
          val model = modelSrv(tpe).getOrElse(sys.error("TODO"))
          model(attrs)
      }
    }
    (entities, total)
  }

  def apply(model: BaseModelDef, queryDef: QueryDef, range: Option[String], sortBy: Seq[String]): (Source[BaseEntity, NotUsed], Future[Long]) = {
    val (src, total) = dbfind(range, sortBy)(indexName ⇒ search(indexName → model.name).query(queryDef.query))
    val entities = src.map(attrs ⇒ model(attrs))
    (entities, total)
  }

  def apply[M <: AbstractModelDef[M, E], E <: BaseEntity](model: M, queryDef: QueryDef, range: Option[String], sortBy: Seq[String]): (Source[E, NotUsed], Future[Long]) = {
    val (src, total) = dbfind(range, sortBy)(indexName ⇒ search(indexName → model.name).query(queryDef.query))
    val entities = src.map(attrs ⇒ model(attrs))
    (entities, total)
  }

  def apply(model: BaseModelDef, queryDef: QueryDef, aggs: Agg*): Future[JsObject] = {
    dbfind(indexName ⇒ search(indexName → model.name).query(queryDef.query).aggregations(aggs.flatMap(_.apply(model))).size(0))
      .map { searchResponse ⇒
        aggs
          .map(_.processResult(model, searchResponse.aggregations))
          .reduceOption(_ ++ _)
          .getOrElse(JsObject(Nil))
      }
  }
}