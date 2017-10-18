package org.elastic4play.services

import com.sksamuel.elastic4s.ElasticDsl.{ boolQuery, existsQuery, hasChildQuery, hasParentQuery, idsQuery, matchAllQuery, nestedQuery, query, rangeQuery, termQuery, termsQuery }
import com.sksamuel.elastic4s.searches.queries.{ BuildableTermsQueryImplicits, QueryDefinition }
import org.apache.lucene.search.join.ScoreMode

import org.elastic4play.models.BaseEntity

object QueryDSL {
  def selectAvg(field: String) = new SelectAvg(field)
  def selectMin(field: String) = new SelectMin(field)
  def selectMax(field: String) = new SelectMax(field)
  def selectSum(field: String) = new SelectSum(field)
  val selectCount: SelectCount.type = SelectCount
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
  def withParent(parent: BaseEntity): QueryDef = withParent(parent.model.name, parent.id)
  def withParent(parentType: String, parentId: String): QueryDef = QueryDef(hasParentQuery(parentType).query(idsQuery(parentId).types(parentType)).scoreMode(false)) // QueryDef(ParentIdQueryDefinition(parentType, parentId)) FIXME doesn't work yet
  def string(queryString: String): QueryDef = QueryDef(query(queryString))
}
