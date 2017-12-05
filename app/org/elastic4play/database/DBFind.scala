package org.elastic4play.database

import javax.inject.{ Inject, Singleton }

import scala.collection.mutable
import scala.concurrent.duration.{ DurationLong, FiniteDuration }
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

import play.api.libs.json.{ JsNull, JsObject, JsString, Json }
import play.api.{ Configuration, Logger }

import akka.NotUsed
import akka.stream.{ Attributes, Materializer, Outlet, SourceShape }
import akka.stream.scaladsl.Source
import akka.stream.stage.{ AsyncCallback, GraphStage, GraphStageLogic, OutHandler }
import com.sksamuel.elastic4s.searches.{ RichSearchHit, RichSearchResponse, SearchBuilderFn, SearchDefinition }

import org.elastic4play.SearchError

/**
  * Service class responsible for entity search
  */
@Singleton
class DBFind(
    pageSize: Int,
    keepAlive: FiniteDuration,
    db: DBConfiguration,
    implicit val ec: ExecutionContext,
    implicit val mat: Materializer) {

  @Inject def this(
      configuration: Configuration,
      db: DBConfiguration,
      ec: ExecutionContext,
      mat: Materializer) =
    this(
      configuration.get[Int]("search.pagesize"),
      configuration.getMillis("search.keepalive").millis,
      db,
      ec,
      mat)

  private[database] val keepAliveStr = keepAlive.toMillis + "ms"
  private[DBFind] lazy val logger = Logger(getClass)

  /**
    * return a new instance of DBFind but using another DBConfiguration
    */
  def switchTo(otherDB: DBConfiguration) = new DBFind(pageSize, keepAlive, otherDB, ec, mat)

  /**
    * Extract offset and limit from optional range
    * Range has the following format : "start-end"
    * If format is invalid of range is None, this function returns (0, 10)
    */
  private[database] def getOffsetAndLimitFromRange(range: Option[String]): (Int, Int) = {
    range match {
      case None        ⇒ (0, 10)
      case Some("all") ⇒ (0, Int.MaxValue)
      case Some(r) ⇒
        val Array(_offset, _end, _*) = (r + "-0").split("-", 3)
        val offset = Try(Math.max(0, _offset.toInt)).getOrElse(0)
        val end = Try(_end.toInt).getOrElse(offset + 10)
        if (end <= offset)
          (offset, 10)
        else
          (offset, end - offset)
    }
  }

  /**
    * Execute the search definition using scroll
    */
  private[database] def searchWithScroll(searchDefinition: SearchDefinition, offset: Int, limit: Int): (Source[RichSearchHit, NotUsed], Future[Long]) = {
    val searchWithScroll = new SearchWithScroll(db, searchDefinition, keepAliveStr, offset, limit)
    (Source.fromGraph(searchWithScroll), searchWithScroll.totalHits)
  }

  /**
    * Execute the search definition
    */
  private[database] def searchWithoutScroll(searchDefinition: SearchDefinition, offset: Int, limit: Int): (Source[RichSearchHit, NotUsed], Future[Long]) = {
    val resp = db.execute(searchDefinition.start(offset).limit(limit))
    val total = resp.map(_.totalHits)
    val src = Source
      .fromFuture(resp)
      .mapConcat { resp ⇒ resp.hits.toList }
    (src, total)
  }

  /**
    * Transform search hit into JsObject
    * This function parses hit source add _type, _routing, _parent and _id attributes
    */
  private[database] def hit2json(hit: RichSearchHit) = {
    val id = JsString(hit.id)
    Json.parse(hit.sourceAsString).as[JsObject] +
      ("_type" → JsString(hit.`type`)) +
      ("_routing" → hit.fields.get("_routing").map(r ⇒ JsString(r.java.getValue[String])).getOrElse(id)) +
      ("_parent" → hit.fields.get("_parent").map(r ⇒ JsString(r.java.getValue[String])).getOrElse(JsNull)) +
      ("_id" → id)
  }

  /**
    * Search entities in ElasticSearch
    *
    * @param range  first and last entities to retrieve, for example "23-42" (default value is "0-10")
    * @param sortBy define order of the entities by specifying field names used in sort. Fields can be prefixed by
    *               "-" for descendant or "+" for ascendant sort (ascendant by default).
    * @param query  a function that build a SearchDefinition using the index name
    * @return Source (akka stream) of JsObject. The source is materialized as future of long that contains the total number of entities.
    */
  def apply(range: Option[String], sortBy: Seq[String])(query: (String) ⇒ SearchDefinition): (Source[JsObject, NotUsed], Future[Long]) = {
    val (offset, limit) = getOffsetAndLimitFromRange(range)
    val sortDef = DBUtils.sortDefinition(sortBy)
    val searchDefinition = query(db.indexName).storedFields("_source", "_routing", "_parent").start(offset).sortBy(sortDef)

    logger.debug(s"search in ${searchDefinition.indexesTypes.indexes.mkString(",")} / ${searchDefinition.indexesTypes.types.mkString(",")} ${SearchBuilderFn(db.client.java, searchDefinition)}")
    val (src, total) = if (limit > 2 * pageSize) {
      searchWithScroll(searchDefinition, offset, limit)
    }
    else {
      searchWithoutScroll(searchDefinition, offset, limit)
    }

    (src.map(hit2json), total)
  }

  /**
    * Execute the search definition
    * This function is used to run aggregations
    */
  def apply(query: (String) ⇒ SearchDefinition): Future[RichSearchResponse] = {
    val searchDefinition = query(db.indexName)
    logger.debug(s"search in ${searchDefinition.indexesTypes.indexes.mkString(",")} / ${searchDefinition.indexesTypes.types.mkString(",")} ${SearchBuilderFn(db.client.java, searchDefinition)}")

    db.execute(searchDefinition)
      .recoverWith {
        case t if DBUtils.isIndexMissing(t) ⇒ Future.failed(t)
        case t                              ⇒ Future.failed(SearchError("Invalid search query", t))
      }
  }
}

class SearchWithScroll(
    db: DBConfiguration,
    searchDefinition: SearchDefinition,
    keepAliveStr: String,
    offset: Int,
    max: Int)(implicit ec: ExecutionContext) extends GraphStage[SourceShape[RichSearchHit]] {

  private[SearchWithScroll] lazy val logger = Logger(getClass)
  val out: Outlet[RichSearchHit] = Outlet[RichSearchHit]("searchHits")
  val shape: SourceShape[RichSearchHit] = SourceShape.of(out)
  val firstResults: Future[RichSearchResponse] = db.execute(searchDefinition.scroll(keepAliveStr))
  val totalHits: Future[Long] = firstResults.map(_.totalHits)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    var processed: Long = 0
    var skip: Int = offset
    val queue: mutable.Queue[RichSearchHit] = mutable.Queue.empty
    var scrollId: Future[String] = firstResults.map(_.scrollId)
    var firstResultProcessed = false

    setHandler(out, new OutHandler {

      def pushNextHit(): Unit = {
        push(out, queue.dequeue())
        processed += 1
        if (processed >= max) {
          completeStage()
        }
      }

      val firstCallback: AsyncCallback[RichSearchResponse] = getAsyncCallback[RichSearchResponse] {
        case searchResponse if skip > 0 ⇒
          if (searchResponse.hits.length <= skip)
            skip -= searchResponse.hits.length
          else {
            queue ++= searchResponse.hits.drop(skip)
            skip = 0
          }
          firstResultProcessed = true
          onPull()
        case searchResponse ⇒
          queue ++= searchResponse.hits
          firstResultProcessed = true
          onPull()
      }

      override def onPull(): Unit = if (firstResultProcessed) {
        import com.sksamuel.elastic4s.ElasticDsl.searchScroll
        if (processed >= max) completeStage()

        if (queue.isEmpty) {
          val callback = getAsyncCallback[Try[RichSearchResponse]] {
            case Success(searchResponse) if searchResponse.isTimedOut ⇒
              logger.warn("Search timeout")
              failStage(SearchError("Request terminated early or timed out", null))
            case Success(searchResponse) if searchResponse.isEmpty ⇒
              completeStage()
            case Success(searchResponse) if skip > 0 ⇒
              if (searchResponse.hits.length <= skip) {
                skip -= searchResponse.hits.length
                onPull()
              }
              else {
                queue ++= searchResponse.hits.drop(skip)
                skip = 0
                pushNextHit()
              }
            case Success(searchResponse) ⇒
              queue ++= searchResponse.hits
              pushNextHit()
            case Failure(error) ⇒
              logger.warn("Search error", error)
              failStage(SearchError("Request terminated early or timed out", error))
          }
          val futureSearchResponse = scrollId.flatMap(s ⇒ db.execute(searchScroll(s).keepAlive(keepAliveStr)))
          scrollId = futureSearchResponse.map(_.scrollId)
          futureSearchResponse.onComplete(callback.invoke)
        }
        else {
          pushNextHit()
        }
      }
      else firstResults.foreach(firstCallback.invoke)
    })
    override def postStop(): Unit = {
      import com.sksamuel.elastic4s.ElasticDsl.clearScroll
      scrollId.foreach { s ⇒ db.execute(clearScroll(s)) }
    }
  }
}