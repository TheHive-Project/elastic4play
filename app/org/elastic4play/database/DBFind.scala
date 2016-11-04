package org.elastic4play.database

import javax.inject.{ Inject, Singleton }

import scala.annotation.implicitNotFound
import scala.collection.mutable
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.{ DurationLong, FiniteDuration }
import scala.util.{ Failure, Success, Try }

import akka.NotUsed
import akka.actor.{ PoisonPill, Props, Stash, actorRef2Scala }
import akka.pattern.ask
import akka.stream.Materializer
import akka.stream.actor.ActorPublisher
import akka.stream.scaladsl.Source
import akka.util.Timeout

import play.api.{ Configuration, Logger }
import play.api.libs.json.{ JsNull, JsObject, JsString, Json }

import com.sksamuel.elastic4s.{ RichSearchHit, RichSearchResponse, SearchDefinition }
import com.sksamuel.elastic4s.ElasticDsl.{ clear, searchScroll }

import org.elastic4play.{ InternalError, SearchError, Timed }

/**
 * Service class responsible for entity search
 */
@Singleton
class DBFind(pageSize: Int,
             keepAlive: FiniteDuration,
             db: DBConfiguration,
             implicit val ec: ExecutionContext,
             implicit val mat: Materializer) {

  @Inject def this(configuration: Configuration,
                   db: DBConfiguration,
                   ec: ExecutionContext,
                   mat: Materializer) =
    this(configuration.getInt("search.pagesize").get,
      configuration.getMilliseconds("search.keepalive").get.millis,
      db,
      ec,
      mat)

  private[database] val indexName = db.indexName
  private[database] val keepAliveStr = keepAlive.toMillis + "ms"
  lazy val log = Logger(getClass)

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
      case None        => (0, 10)
      case Some("all") => (0, Int.MaxValue)
      case Some(r) =>
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
  private[database] def searchWithScroll(searchDefinition: SearchDefinition, limit: Int): (Source[RichSearchHit, NotUsed], Future[Long]) = {
    implicit val timeout = Timeout(keepAlive)
    import akka.stream.scaladsl._

    val (actorRef, pub) = Source.actorPublisher[RichSearchHit](
      Props(
        classOf[SearchPublisher],
        db,
        searchDefinition limit pageSize,
        keepAliveStr,
        limit))
      .toMat(Sink.asPublisher(true))(Keep.both)
      .run()
    val total = (actorRef ? SearchPublisher.Start).flatMap {
      case Success(l: Long) => Future.successful(l)
      case Failure(t)       => Future.failed(t)
      case r                => Future.failed(InternalError(s"Unexpected actor response : %r"))
    }
    (Source.fromPublisher(pub), total)
  }

  /**
   * Execute the search definition
   */
  private[database] def searchWithoutScroll(searchDefinition: SearchDefinition, limit: Int): (Source[RichSearchHit, NotUsed], Future[Long]) = {
    val resp = db.execute(searchDefinition limit limit)
    val total = resp.map(_.totalHits)
    val src = Source
      .fromFuture(resp)
      .mapConcat { resp => resp.hits.toList }
    (src, total)
  }

  /**
   * Transform search hit into JsObject
   * This function parses hit source add _type, _routing, _parent and _id attributes
   */
  private[database] def hit2json(hit: RichSearchHit) = {
    val id = JsString(hit.id)
    Json.parse(hit.sourceAsString).as[JsObject] +
      ("_type" -> JsString(hit.`type`)) +
      ("_routing" -> hit.fields.get("_routing").map(r => JsString(r.value[String])).getOrElse(id)) +
      ("_parent" -> hit.fields.get("_parent").map(r => JsString(r.value[String])).getOrElse(JsNull)) +
      ("_id" -> id)
  }

  /**
   * Search entities in ElasticSearch
   * @param range first and last entities to retrieve, for example "23-42" (default value is "0-10")
   * @param sortBy define order of the entities by specifying field names used in sort. Fields can be prefixed by
   * "-" for descendant or "+" for ascendant sort (ascendant by default).
   * @param query a function that build a SearchDefinition using the index name
   * @return Source (akka stream) of JsObject. The source is materialized as future of long that contains the total number of entities.
   */
  def apply(range: Option[String], sortBy: Seq[String])(query: (String) => SearchDefinition): (Source[JsObject, NotUsed], Future[Long]) = {
    val (offset, limit) = getOffsetAndLimitFromRange(range)
    val sortDef = DBUtils.sortDefinition(sortBy)
    val searchDefinition = query(indexName) fields ("_source", "_routing", "_parent") start offset sort (sortDef: _*)

    log.debug(s"search ${searchDefinition._builder}")
    val (src, total) = if (limit > pageSize) {
      searchWithScroll(searchDefinition, limit)
    } else {
      searchWithoutScroll(searchDefinition, limit)
    }

    (src.map(hit2json), total)
  }

  /**
   * Execute the search definition
   * This function is used to run aggregations
   */
  def apply(query: (String) => SearchDefinition) = {
    val searchDefinition = query(indexName)
    db.execute(searchDefinition)
      .recoverWith {
        case t if DBUtils.isIndexMissing(t) => Future.failed(t)
        case t                              => Future.failed(SearchError("Invalid search query", t))
      }
  }
}

/**
 * This object defines messages specific to SearchPublisher
 */
object SearchPublisher {
  /**
   * Message used to start the search
   */
  object Start
}

/**
 * Actor used as stream publisher
 */
class SearchPublisher(
    db: DBConfiguration,
    searchDefinition: SearchDefinition,
    keepAliveStr: String,
    max: Int) extends ActorPublisher[RichSearchHit] with Stash {
  import SearchPublisher._
  import context.dispatcher
  import akka.stream.actor.ActorPublisherMessage._
  private val queue: mutable.Queue[RichSearchHit] = mutable.Queue.empty
  private var processed: Long = 0
  private var scrollId: Option[String] = None
  lazy val log = Logger(getClass)

  /**
   * initial state of the actor
   * It can only receive "Start" message. All other messages are stashed
   */
  def receive = {
    case Start =>
      val _sender = sender
      db.execute(searchDefinition.scroll(keepAliveStr)).onComplete {
        case Success(result) =>
          scrollId = result.scrollIdOpt
          _sender ! Success(result.totalHits)
          self ! result

        case f @ Failure(t) =>
          _sender ! f
          onError(t)
          self ! PoisonPill
      }
      context become fetching
      unstashAll()
    case _ =>
      stash()
  }

  /**
   * In this state, actor waits for subscriber request
   * If the queue is not empty send entities
   * If the queue is not enough change
   */
  def ready: Receive = {
    case Request(n) if n > queue.size =>
      require(scrollId.isDefined)
      db.execute(searchScroll(scrollId.get).keepAlive(keepAliveStr)).onComplete {
        case Success(result) =>
          self ! result
        case Failure(t) =>
          onError(t)
          self ! PoisonPill
      }
      context become fetching
      self ! Request(n - queue.size)
      send(queue.size.toLong)
    case Request(n) =>
      send(n)
  }

  /**
   * In this state, actor retrieve the next page of result using scroll
   * and add in local queue
   */
  def fetching: Receive = {
    case Request(n) =>
      require(queue.isEmpty) // must be empty or why did we not send it before switching to this mode?
      stash()
    case resp: RichSearchResponse if resp.isTimedOut =>
      onError(SearchError("Request terminated early or timed out", null))
      context.stop(self)
    case resp: RichSearchResponse if resp.isEmpty =>
      onComplete()
      context.stop(self)
    case resp: RichSearchResponse =>
      queue.enqueue(resp.hits: _*)
      context become ready
      unstashAll()
  }

  private def send(k: Long): Unit = {
    require(queue.size >= k)
    for (_ <- 0l until k) {
      if (max == 0 || processed < max) {
        onNext(queue.dequeue())
        processed = processed + 1
        if (processed == max && max > 0) {
          onComplete()
          scrollId.foreach { s => db.execute(clear scroll s) }
          context.stop(self)
        }
      }
    }
  }

}