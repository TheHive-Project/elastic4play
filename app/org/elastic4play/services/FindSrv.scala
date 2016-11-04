package org.elastic4play.services

import javax.inject.{ Inject, Singleton }

import scala.collection.JavaConversions.{ asScalaBuffer, seqAsJavaList }
import scala.concurrent.{ ExecutionContext, Future }
import scala.language.reflectiveCalls
import scala.math.BigDecimal.{ double2bigDecimal, int2bigDecimal, long2bigDecimal }
import scala.util.Try

import akka.NotUsed
import akka.stream.scaladsl.Source

import play.api.libs.json.{ JsArray, JsNumber, JsObject }
import play.api.libs.json.JsValue.jsValueToJsLookup

import org.elastic4play.BadRequestError
import org.elastic4play.database.{ DBConfiguration, DBFind, DBUtils }
import org.elastic4play.models.{ AbstractModelDef, BaseEntity, BaseModelDef }
import org.elastic4play.utils.Date.RichJoda
import org.elasticsearch.search.aggregations.Aggregations
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

import com.sksamuel.elastic4s.{ QueryDefinition, RichSearchHit }
import com.sksamuel.elastic4s.AbstractAggregationDefinition
import com.sksamuel.elastic4s.ElasticDsl.{ aggregation, bool, existsQuery, hasChildQuery, hasParentQuery, idsQuery, matchAllQuery, must, nestedQuery, not => _not, query, rangeQuery, search, should, termQuery, termsQuery }
import com.sksamuel.elastic4s.IndexesAndTypes.apply
import com.sksamuel.elastic4s.ScriptDefinition.string2Script
import com.sksamuel.elastic4s.{ SearchType, ValuesSourceMetricsAggregationDefinition }

case class QueryDef(query: QueryDefinition)

trait Agg {
  def apply(model: BaseModelDef): Seq[AbstractAggregationDefinition]
  def processResult(model: BaseModelDef, aggregations: Aggregations): JsObject
}

trait FieldSelectable { self: Agg =>
  val aggFunction: ValuesSourceMetricsAggregationDefinition[_, _]
  val field: String
  def apply(model: BaseModelDef) = {
    field.split("\\.", 3) match {
      case Array("computed", c) =>
        val script = model.computedMetrics.getOrElse(
          c,
          throw BadRequestError(s"Field $field is unknown in ${model.name}"))
        Seq(aggFunction.script(script).asInstanceOf[AbstractAggregationDefinition])
      case array =>
        val attribute = model.attributes.find(_.name == array(0)).getOrElse {
          throw BadRequestError(s"Field $field is unknown in ${model.name}")
        }
        // TODO check attribute type
        Seq(aggFunction.field(field).asInstanceOf[AbstractAggregationDefinition])
    }
  }
}

class SelectAvg(val field: String) extends Agg with FieldSelectable {
  val name = s"avg_$field"
  val aggFunction = aggregation avg name
  def processResult(model: BaseModelDef, aggregations: Aggregations): JsObject = {
    val avg = aggregations.get[Avg](name)
    val value = Try(JsNumber(avg.getValue)).toOption.getOrElse(JsNumber(0))
    JsObject(Seq(avg.getName -> value))
  }
}

class SelectMin(val field: String) extends Agg with FieldSelectable {
  val name = s"min_$field"
  val aggFunction = aggregation min name
  def processResult(model: BaseModelDef, aggregations: Aggregations): JsObject = {
    val min = aggregations.get[Min](name)
    val value = Try(JsNumber(min.getValue)).toOption.getOrElse(JsNumber(0))
    JsObject(Seq(min.getName -> value))
  }
}

class SelectMax(val field: String) extends Agg with FieldSelectable {
  val name = s"max_$field"
  val aggFunction = aggregation max name
  def processResult(model: BaseModelDef, aggregations: Aggregations): JsObject = {
    val max = aggregations.get[Max](name)
    val value = Try(JsNumber(max.getValue)).toOption.getOrElse(JsNumber(0))
    JsObject(Seq(max.getName -> value))
  }
}

class SelectSum(val field: String) extends Agg with FieldSelectable {
  val name = s"sum_$field"
  val aggFunction = aggregation sum name
  def processResult(model: BaseModelDef, aggregations: Aggregations): JsObject = {
    val sum = aggregations.get[Sum](name)
    val value = Try(JsNumber(sum.getValue)).toOption.getOrElse(JsNumber(0))
    JsObject(Seq(sum.getName -> value))
  }
}

object SelectCount extends Agg {
  val name = "count"
  override def apply(model: BaseModelDef) = Seq(aggregation filter name filter matchAllQuery)
  def processResult(model: BaseModelDef, aggregations: Aggregations): JsObject = {
    val count = aggregations.get[Filter](name)
    JsObject(Seq(count.getName -> JsNumber(count.getDocCount)))
  }
}

class SelectTop(size: Int, sortBy: Seq[String]) extends Agg {
  val name = "top"
  def apply(model: BaseModelDef) = Seq(aggregation topHits name size size sort (DBUtils.sortDefinition(sortBy): _*))
  def processResult(model: BaseModelDef, aggregations: Aggregations): JsObject = {
    val top = aggregations.get[TopHits](name)
    // "top" -> JsArray(top.getHits.getHits.map(h => FindSrv.hit2json(RichSearchHit(h))))
    JsObject(Seq("top" -> JsArray(top.getHits.getHits.map(h => DBUtils.hit2json(None, new RichSearchHit(h)))))) // FIXME migration for ElasticSearch 2.x
  }
}

class GroupByCategory(categories: Map[String, QueryDef], subAggs: Seq[Agg]) extends Agg {
  val name = "categories"
  def apply(model: BaseModelDef): Seq[AbstractAggregationDefinition] = {
    Seq(categories.foldLeft(aggregation filters name) {
      case (catAgg, (catName, catQuery)) => catAgg filter (catName, catQuery.query)
    } aggregations subAggs.flatMap(_.apply(model)))
  }
  def processResult(model: BaseModelDef, aggregations: Aggregations): JsObject = {
    val filters = aggregations.get[Filters](name)
    JsObject {
      categories.keys.toSeq.map { cat =>
        val subAggResults = filters.getBucketByKey(cat).getAggregations
        cat -> subAggs.map(_.processResult(model, subAggResults))
          .reduceOption(_ ++ _)
          .getOrElse(JsObject(Nil))
      }
    }
  }

}
class GroupByTime(fields: Seq[String], interval: String, subAggs: Seq[Agg]) extends Agg {
  def apply(model: BaseModelDef) = {
    fields.map { f => aggregation datehistogram s"datehistogram_$f" field f interval new DateHistogramInterval(interval) aggregations subAggs.flatMap(_.apply(model)) }
  }
  def processResult(model: BaseModelDef, aggregations: Aggregations): JsObject = {
    val aggs = fields.map { f =>
      val buckets = aggregations.get[Histogram](s"datehistogram_$f").getBuckets
      f -> (buckets.map { bucket =>
        val results = subAggs
          .map(_.processResult(model, bucket.getAggregations))
          .reduceOption(_ ++ _)
          .getOrElse(JsObject(Nil))
        // date -> obj(key{avg, min} -> value)
        (bucket.getKey.asInstanceOf[DateTime].toIso -> results)
      }.toMap)
    }.toMap
    val keys = aggs.values.flatMap(_.keys).toSet
    JsObject {
      keys.map { date =>
        date -> JsObject(aggs.map {
          case (df, values) =>
            df -> values.getOrElse(date, JsObject(Nil))
        })
      }.toMap
    }
  }
}
class GroupByField(field: String, size: Option[Int], sortBy: Seq[String], subAggs: Seq[Agg]) extends Agg {
  def apply(model: BaseModelDef) = {
    Seq(aggregation terms s"term_$field" field field aggregations subAggs.flatMap(_.apply(model)))
      .map { agg => size.fold(agg)(s => agg.size(s)) }
      .map {
        case agg if sortBy.isEmpty => agg
        case agg =>
          agg.order(Order.compound(sortBy.flatMap {
            case f if f.startsWith("+") => Some(Order.aggregation(f.drop(1), true))
            case f if f.startsWith("-") => Some(Order.aggregation(f.drop(1), false))
            case f if f.length() > 0    => Some(Order.aggregation(f, true))
          }))
      }
  }
  def processResult(model: BaseModelDef, aggregations: Aggregations): JsObject = {
    val buckets = aggregations.get[Terms](s"term_$field").getBuckets
    JsObject {
      buckets.map { bucket =>
        val results = subAggs
          .map(_.processResult(model, bucket.getAggregations))
          .reduceOption(_ ++ _)
          .getOrElse(JsObject(Nil))
        (bucket.getKeyAsString -> results)
      }.toMap
    }
  }
}
object QueryDSL {
  def selectAvg(field: String) = new SelectAvg(field)
  def selectMin(field: String) = new SelectMin(field)
  def selectMax(field: String) = new SelectMax(field)
  def selectSum(field: String) = new SelectSum(field)
  def selectCount = SelectCount
  def selectTop(size: Int, sortBy: Seq[String]) = new SelectTop(size, sortBy)
  def groupByTime(fields: Seq[String], interval: String, selectables: Agg*) = new GroupByTime(fields, interval, selectables)
  def groupByField(field: String, selectables: Agg*) = new GroupByField(field, None, Nil, selectables)
  def groupByField(field: String, size: Int, sortBy: Seq[String], selectables: Agg*) = new GroupByField(field, Some(size), sortBy, selectables)
  def groupByCaterogy(categories: Map[String, QueryDef], selectables: Agg*) = new GroupByCategory(categories, selectables)

  private def nestedField(field: String, q: (String) => QueryDefinition) = {
    val names = field.split("\\.")
    names.init.foldRight(q(field)) {
      case (subName, queryDef) => nestedQuery(subName) query (queryDef)
    }
  }

  implicit class SearchField(field: String) {
    def ~=(value: Any) = QueryDef(nestedField(field, termQuery(_, value)))
    def ~!=(value: Any) = not(QueryDef(nestedField(field, termQuery(_, value))))
    def ~<(value: Any) = QueryDef(nestedField(field, rangeQuery(_) to (value) includeUpper (false)))
    def ~>(value: Any) = QueryDef(nestedField(field, rangeQuery(_) from (value) includeLower (false)))
    def ~<=(value: Any) = QueryDef(nestedField(field, rangeQuery(_) to (value) includeUpper (true)))
    def ~>=(value: Any) = QueryDef(nestedField(field, rangeQuery(_) from (value) includeLower (true)))
    def ~<>(value: (Any, Any)) = QueryDef(nestedField(field, rangeQuery(_) from (value._1) to (value._2) includeUpper (false) includeLower (false)))
    def ~=<>=(value: (Any, Any)) = QueryDef(nestedField(field, rangeQuery(_) from (value._1) to (value._2) includeUpper (true) includeLower (true)))
    def in(values: AnyRef*) = QueryDef(nestedField(field, termsQuery(_, values: _*)))
  }

  def ofType(value: String) = QueryDef(termQuery("_type", value))
  def withId(entityIds: String*): QueryDef = QueryDef(idsQuery(entityIds))
  def any: QueryDef = QueryDef(matchAllQuery)
  def contains(field: String): QueryDef = QueryDef(nestedField(field, existsQuery(_)))
  def or(queries: QueryDef*): QueryDef = or(queries)
  def or(queries: Iterable[QueryDef]): QueryDef = QueryDef(bool { should { queries.map(_.query) } })
  def and(queries: QueryDef*): QueryDef = QueryDef(bool { must { queries.map(_.query) } })
  def and(queries: Iterable[QueryDef]): QueryDef = QueryDef(bool { must { queries.map(_.query) } })
  def not(query: QueryDef): QueryDef = QueryDef(bool { _not { query.query } })
  def child(childType: String, query: QueryDef): QueryDef = QueryDef(hasChildQuery(childType) query query.query)
  def parent(parentType: String, query: QueryDef): QueryDef = QueryDef(hasParentQuery(parentType) query query.query)
  def string(queryString: String): QueryDef = QueryDef(query(queryString))
}

@Singleton
class FindSrv @Inject() (dbfind: DBFind,
                         modelSrv: ModelSrv,
                         implicit val ec: ExecutionContext) {

  def switchTo(db: DBConfiguration) = new FindSrv(dbfind.switchTo(db), modelSrv, ec)

  def apply(modelName: Option[String], queryDef: QueryDef, range: Option[String], sortBy: Seq[String]): (Source[BaseEntity, NotUsed], Future[Long]) = {
    val (src, total) = dbfind(range, sortBy)(indexName => modelName.fold(search in indexName)(m => search in indexName -> m) query queryDef.query)
    val entities = src.map { attrs =>
      modelName match {
        //case Some("audit") => auditModel.get()(attrs)
        case Some(m) => modelSrv(m).getOrElse(sys.error("TODO"))(attrs)
        case None =>
          val tpe = (attrs \ "_type").asOpt[String].getOrElse(sys.error("TODO"))
          val model = modelSrv(tpe).getOrElse(sys.error("TODO"))
          model(attrs)
      }
    }
    (entities, total)
  }

  def apply(model: BaseModelDef, queryDef: QueryDef, range: Option[String], sortBy: Seq[String]): (Source[BaseEntity, NotUsed], Future[Long]) = {
    val (src, total) = dbfind(range, sortBy)(indexName => search in indexName -> model.name query queryDef.query)
    val entities = src.map(attrs => model(attrs))
    (entities, total)
  }

  def apply[M <: AbstractModelDef[M, E], E <: BaseEntity](model: M, queryDef: QueryDef, range: Option[String], sortBy: Seq[String]): (Source[E, NotUsed], Future[Long]) = {
    val (src, total) = dbfind(range, sortBy)(indexName => search in indexName -> model.name query queryDef.query)
    val entities = src.map(attrs => model(attrs).asInstanceOf[E])
    (entities, total)
  }

  def apply(model: BaseModelDef, queryDef: QueryDef, aggs: Agg*): Future[JsObject] = {
    dbfind(indexName => search in indexName -> model.name query queryDef.query aggregations aggs.flatMap(_.apply(model)) searchType SearchType.QueryAndFetch size 0)
      .map {
        case searchResponse => aggs
          .map(_.processResult(model, searchResponse.aggregations))
          .reduceOption(_ ++ _)
          .getOrElse(JsObject(Nil))
      }
  }
}