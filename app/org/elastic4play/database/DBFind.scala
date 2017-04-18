package org.elastic4play.database

import javax.inject.{ Inject, Singleton }

import akka.NotUsed
import akka.actor.{ PoisonPill, Props, Stash, actorRef2Scala }
import akka.pattern.ask
import akka.stream.Materializer
import akka.stream.actor.ActorPublisher
import akka.stream.scaladsl.Source
import akka.util.Timeout
import com.sksamuel.elastic4s.ElasticDsl.{ clear, searchScroll }
import com.sksamuel.elastic4s.{ RichSearchHit, RichSearchResponse, SearchDefinition }
import org.elastic4play.{ InternalError, SearchError }
import play.api.libs.json.{ JsNull, JsObject, JsString, Json }
import play.api.{ Configuration, Logger }

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration.{ DurationLong, FiniteDuration }
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

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
      configuration.getInt("search.pagesize").get,
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
    implicit val timeout = Timeout(keepAlive)
    import akka.stream.scaladsl._

    val (actorRef, pub) = Source.actorPublisher[RichSearchHit](
      Props(
        classOf[SearchPublisher],
        db,
        searchDefinition limit pageSize,
        keepAliveStr,
        offset,
        limit))
      .toMat(Sink.asPublisher(true))(Keep.both)
      .run()
    val total = (actorRef ? SearchPublisher.Start).flatMap {
      case Success(l: Long) ⇒ Future.successful(l)
      case Failure(t)       ⇒ Future.failed(t)
      case _                ⇒ Future.failed(InternalError(s"Unexpected actor response : %r"))
    }
    (Source.fromPublisher(pub), total)
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
      ("_routing" → hit.fields.get("_routing").map(r ⇒ JsString(r.value[String])).getOrElse(id)) +
      ("_parent" → hit.fields.get("_parent").map(r ⇒ JsString(r.value[String])).getOrElse(JsNull)) +
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
    val searchDefinition = query(indexName).fields("_source", "_routing", "_parent").sort(sortDef: _*)

    log.debug(s"search ${searchDefinition._builder}")
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
    val searchDefinition = query(indexName)
    db.execute(searchDefinition)
      .recoverWith {
        case t if DBUtils.isIndexMissing(t) ⇒ Future.failed(t)
        case t                              ⇒ Future.failed(SearchError("Invalid search query", t))
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
    offset: Int,
    max: Int) extends ActorPublisher[RichSearchHit] with Stash {

  import SearchPublisher._
  import akka.stream.actor.ActorPublisherMessage._
  import context.dispatcher

  private val queue: mutable.Queue[RichSearchHit] = mutable.Queue.empty
  private var processed: Long = 0
  private var scrollId: Option[String] = None
  private[SearchPublisher] lazy val logger = Logger(getClass)

  override def postStop(): Unit = {
    scrollId.foreach { s ⇒ db.execute(clear scroll s) }
  }

  /**
   * initial state of the actor
   * It can only receive "Start" message. All other messages are stashed
   */
  override def receive: Receive = {
    case Start ⇒
      logger.trace(s"$self: Start search")
      val _sender = sender
      db.execute(searchDefinition.scroll(keepAliveStr)).onComplete {
        case Success(result) ⇒
          logger.trace(s"$self: Send success")
          _sender ! Success(result.totalHits)
          self ! result

        case f: Failure[_] ⇒
          logger.trace(s"$self: Send failure")
          _sender ! f
          self ! f
      }
      context become fetching(offset)
      logger.trace(s"$self: become fetching($offset)")
      unstashAll()
    case _ ⇒
      stash()
  }

  /**
   * In this state, actor waits for subscriber request
   * If the queue is not empty send entities
   * If the queue is not enough change
   */
  def ready(remainingOffset: Int): Receive = {
    case Request(n) if n <= queue.size ⇒
      logger.trace(s"$self/ready($remainingOffset): request $n element(s), queue size is enough")
      if (!send(n)) {
        logger.trace(s"$self/ready($remainingOffset): search is over (stream acive is $isActive)")
        if (isActive) onComplete()
        self ! PoisonPill
      }
    case Request(n) if n > queue.size ⇒
      logger.trace(s"$self/ready($remainingOffset): request $n element(s), queue size is not enough")
      require(scrollId.isDefined)
      val l = queue.size
      if (send(l.toLong)) {
        logger.trace(s"$self/ready($remainingOffset): search next page, become fetching($remainingOffset) then request ${n - l}")
        db.execute(searchScroll(scrollId.get).keepAlive(keepAliveStr)).onComplete {
          case Success(result) ⇒ self ! result
          case f: Failure[_]   ⇒ self ! f
        }

        context become fetching(remainingOffset)
        self ! Request(n - l)
      }
      else {
        logger.trace(s"$self/ready($remainingOffset): search is over (stream acive is $isActive)")
        if (isActive) onComplete()
        self ! PoisonPill

      }

    case Failure(t) ⇒
      logger.trace(s"$self/ready($remainingOffset): receive failure")
      if (isActive) onError(t)
      self ! PoisonPill
    case other ⇒
      logger.trace(s"$self/ready($remainingOffset): unexpected message: $other")
  }

  /**
   * In this state, actor retrieve the next page of result using scroll
   * and add in local queue
   */
  def fetching(remainingOffset: Int): Receive = {
    case Request(_) ⇒
      logger.trace(s"$self/fetching($remainingOffset): queue contains ${queue.size} element(s), should be 0")
      require(queue.isEmpty) // must be empty or why did we not send it before switching to this mode?
      stash()
    case resp: RichSearchResponse if resp.isTimedOut ⇒
      logger.trace(s"$self/fetching($remainingOffset): search fails with timeout")
      self ! Failure(SearchError("Request terminated early or timed out", null))
    case resp: RichSearchResponse if resp.isEmpty ⇒
      logger.trace(s"$self/fetching($remainingOffset): search is over (stream active is $isActive)")
      if (isActive) onComplete()
      self ! PoisonPill
    case resp: RichSearchResponse ⇒
      scrollId = resp.scrollIdOpt
      val l = resp.hits.length
      logger.trace(s"$self/fetching($remainingOffset): receive $l result(s), queue size is ${queue.size} scrollId=$scrollId")
      if (l > remainingOffset) {
        queue.enqueue(resp.hits.drop(remainingOffset): _*)
        logger.trace(s"$self/fetching($remainingOffset): enqueue results expect $l, queue size is ${queue.size}, become ready(0)")
        context become ready(0)
      }
      else {
        logger.trace(s"$self/fetching($remainingOffset): become ready(${remainingOffset - l})")
        context become ready(remainingOffset - l)
      }
      unstashAll()
    case Failure(t) ⇒
      logger.trace(s"$self/fetching($remainingOffset): receive failure")
      if (isActive) onError(t)
      self ! PoisonPill
    case other ⇒
      logger.trace(s"$self/fetching($remainingOffset): unexpected message: $other")
  }

  @tailrec
  private def send(k: Long): Boolean = {
    if (k > 0) {
      require(queue.nonEmpty)
      if (max == 0 || processed < max) {
        onNext(queue.dequeue())
        processed = processed + 1
        send(k - 1)
      }
      else
        false
    }
    else (max == 0 || processed < max)
  }
}