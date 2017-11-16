package org.elastic4play.services

import com.sksamuel.elastic4s.ElasticDsl.{ boolQuery, existsQuery, hasChildQuery, hasParentQuery, idsQuery, matchAllQuery, nestedQuery, query, rangeQuery, termQuery, termsQuery }
import com.sksamuel.elastic4s.searches.queries.{ BuildableTermsQueryImplicits, QueryDefinition }
import org.apache.lucene.search.join.ScoreMode

import org.elastic4play.models.BaseEntity

object QueryDSL {
  def selectAvg(aggregationName: Option[String], field: String, query: Option[QueryDef]): SelectAvg = new SelectAvg(aggregationName.getOrElse(s"avg_$field"), field, query)
  def selectAvg(field: String): SelectAvg = selectAvg(None, field, None)
  def selectMin(aggregationName: Option[String], field: String, query: Option[QueryDef]): SelectMin = new SelectMin(aggregationName.getOrElse(s"min_$field"), field, query)
  def selectMin(field: String): SelectMin = selectMin(None, field, None)
  def selectMax(aggregationName: Option[String], field: String, query: Option[QueryDef]): SelectMax = new SelectMax(aggregationName.getOrElse(s"max_$field"), field, query)
  def selectMax(field: String): SelectMax = selectMax(None, field, None)
  def selectSum(aggregationName: Option[String], field: String, query: Option[QueryDef]): SelectSum = new SelectSum(aggregationName.getOrElse(s"sum_$field"), field, query)
  def selectSum(field: String): SelectSum = selectSum(None, field, None)
  def selectCount(aggregationName: Option[String], query: Option[QueryDef]): SelectCount = new SelectCount(aggregationName.getOrElse("count"), query)
  val selectCount: SelectCount = selectCount(None, None)
  def selectTop(aggregationName: Option[String], size: Int, sortBy: Seq[String]): SelectTop = new SelectTop(aggregationName.getOrElse("top"), size, sortBy)
  def selectTop(size: Int, sortBy: Seq[String]): SelectTop = selectTop(None, size, sortBy)
  def groupByTime(aggregationName: Option[String], fields: Seq[String], interval: String, selectables: Agg*): GroupByTime = new GroupByTime(aggregationName.getOrElse("datehistogram"), fields, interval, selectables)
  def groupByTime(fields: Seq[String], interval: String, selectables: Agg*): GroupByTime = groupByTime(None, fields, interval, selectables: _*)
  def groupByField(aggregationName: Option[String], field: String, size: Int, sortBy: Seq[String], selectables: Agg*): GroupByField = new GroupByField(aggregationName.getOrElse("term"), field, Some(size), sortBy, selectables)
  def groupByField(aggregationName: Option[String], field: String, selectables: Agg*): GroupByField = new GroupByField(aggregationName.getOrElse("term"), field, None, Nil, selectables)
  def groupByField(field: String, selectables: Agg*): GroupByField = groupByField(None, field, selectables: _*)

  def groupByCaterogy(aggregationName: Option[String], categories: Map[String, QueryDef], selectables: Agg*) = new GroupByCategory(aggregationName.getOrElse("categories"), categories, selectables)

  private def nestedField(field: String, q: (String) ⇒ QueryDefinition) = {
    val names = field.split("\\.")
    names.init.foldRight(q(field)) {
      case (subName, queryDef) ⇒ nestedQuery(subName).query(queryDef).scoreMode(ScoreMode.None)
    }
  }

  implicit class SearchField(field: String) extends BuildableTermsQueryImplicits {
    private def convertValue(value: Any): Any = value match {
      case _: Enumeration#Value ⇒ value.toString
      case bd: BigDecimal       ⇒ bd.toDouble
      case _                    ⇒ value
    }
    def ~=(value: Any) = QueryDef(nestedField(field, termQuery(_, convertValue(value))))
    def ~!=(value: Any): QueryDef = not(QueryDef(nestedField(field, termQuery(_, convertValue(value)))))
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
  def withParent(parent: BaseEntity): QueryDef = withParent(parent.model.modelName, parent.id)
  def withParent(parentType: String, parentId: String): QueryDef = QueryDef(hasParentQuery(parentType).query(idsQuery(parentId).types(parentType)).scoreMode(false)) // QueryDef(ParentIdQueryDefinition(parentType, parentId)) FIXME doesn't work yet
  def string(queryString: String): QueryDef = QueryDef(query(queryString))
}
