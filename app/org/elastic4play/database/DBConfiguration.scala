package org.elastic4play.database

import javax.inject.{ Inject, Named, Singleton }

import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.concurrent.duration._

import play.api.inject.ApplicationLifecycle
import play.api.{ Configuration, Logger }

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Sink, Source }
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.admin.IndicesExists
import com.sksamuel.elastic4s.cluster.ClusterHealthDefinition
import com.sksamuel.elastic4s.delete.DeleteByIdDefinition
import com.sksamuel.elastic4s.get.GetDefinition
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.{ HttpClient, RequestFailure, RequestSuccess }
import com.sksamuel.elastic4s.http.bulk.BulkResponseItem
import com.sksamuel.elastic4s.http.cluster.ClusterHealthResponse
import com.sksamuel.elastic4s.http.delete.DeleteResponse
import com.sksamuel.elastic4s.http.get.GetResponse
import com.sksamuel.elastic4s.http.index.admin.IndexExistsResponse
import com.sksamuel.elastic4s.http.index.{ CreateIndexResponse, IndexResponse }
import com.sksamuel.elastic4s.http.search.{ ClearScrollResponse, SearchHit, SearchResponse }
import com.sksamuel.elastic4s.http.update.UpdateResponse
import com.sksamuel.elastic4s.indexes.{ CreateIndexDefinition, IndexDefinition }
import com.sksamuel.elastic4s.searches._
import com.sksamuel.elastic4s.streams.{ RequestBuilder, ResponseListener }
import com.sksamuel.elastic4s.update.UpdateDefinition
import com.sksamuel.elastic4s.streams.ReactiveElastic._

import org.elastic4play.{ SearchError, Timed }

/**
  * This class is a wrapper of ElasticSearch client from Elastic4s
  * It builds the client using configuration (ElasticSearch addresses, cluster and index name)
  * It add timed annotation in order to measure storage metrics
  */
@Singleton
class DBConfiguration(
    searchHost: Seq[String],
    searchCluster: String,
    baseIndexName: String,
    xpackUsername: Option[String], // FIXME
    xpackPassword: Option[String], //FIXME
    lifecycle: ApplicationLifecycle,
    val version: Int,
    implicit val ec: ExecutionContext,
    implicit val actorSystem: ActorSystem) {

  @Inject() def this(
      configuration: Configuration,
      lifecycle: ApplicationLifecycle,
      @Named("databaseVersion") version: Int,
      ec: ExecutionContext,
      actorSystem: ActorSystem) = {
    this(
      configuration.get[Seq[String]]("search.host"),
      configuration.get[String]("search.cluster"),
      configuration.get[String]("search.index"),
      configuration.getOptional[String]("search.username"),
      configuration.getOptional[String]("search.password"),
      lifecycle,
      version,
      ec,
      actorSystem)
  }

  private[DBConfiguration] lazy val logger = Logger(getClass)

  // FIXME
  //  private def connect(): TcpClient = {
  //    val uri = ElasticsearchClientUri(s"elasticsearch://${searchHost.mkString(",")}")
  //    val settings = Settings.builder()
  //    settings.put("cluster.name", searchCluster)
  //
  //    val xpackClient = for {
  //      username ← xpackUsername
  //      if username.nonEmpty
  //      password ← xpackPassword
  //      if password.nonEmpty
  //      _ = settings.put("xpack.security.user", s"$username:$password")
  //    } yield XPackElasticClient(settings.build(), uri)
  //
  //    xpackClient.getOrElse(TcpClient.transport(settings.build(), uri))
  //  }

  /**
    * Underlying ElasticSearch client
    */
  private[database] val client = {
    val uri = ElasticsearchClientUri("elasticsearch://" + searchHost.mkString(","))
    HttpClient(uri.copy(options = uri.options + ("cluster.name" -> searchCluster)))
  }
  // when application close, close also ElasticSearch connection
  lifecycle.addStopHook { () ⇒ Future { client.close() } }

  private def processResponse[R](response: Future[Either[RequestFailure, RequestSuccess[R]]]): Future[R] = {
    response.flatMap { response ⇒
      response.fold[Future[R]](
        r ⇒ Future.failed(SearchError(r.error)),
        r ⇒ Future.successful(r.result))
    }
  }
  @Timed("database.index")
  def execute(indexDefinition: IndexDefinition): Future[IndexResponse] = {
    processResponse(client.execute(indexDefinition))
  }
  @Timed("database.search")
  def execute(searchDefinition: SearchDefinition): Future[SearchResponse] = {
    processResponse(client.execute(searchDefinition))
  }
  @Timed("database.create")
  def execute(createIndexDefinition: CreateIndexDefinition): Future[CreateIndexResponse] = {
    processResponse(client.execute(createIndexDefinition))
  }
  @Timed("database.update")
  def execute(updateDefinition: UpdateDefinition): Future[UpdateResponse] = {
    processResponse(client.execute(updateDefinition))
  }
  @Timed("database.search_scroll")
  def execute(searchScrollDefinition: SearchScrollDefinition): Future[SearchResponse] = {
    processResponse(client.execute(searchScrollDefinition))
  }
  @Timed("database.index_exists")
  def execute(indicesExists: IndicesExists): Future[IndexExistsResponse] = {
    processResponse(client.execute(indicesExists))
  }
  @Timed("database.delete")
  def execute(deleteByIdDefinition: DeleteByIdDefinition): Future[DeleteResponse] = {
    processResponse(client.execute(deleteByIdDefinition))
  }
  @Timed("database.get")
  def execute(getDefinition: GetDefinition): Future[Either[RequestFailure, RequestSuccess[GetResponse]]] = client.execute(getDefinition)
  @Timed("database.clear_scroll")
  def execute(clearScrollDefinition: ClearScrollDefinition): Future[Either[RequestFailure, RequestSuccess[ClearScrollResponse]]] = client.execute(clearScrollDefinition)
  @Timed("database.cluster_health")
  def execute(clusterHealthDefinition: ClusterHealthDefinition): Future[ClusterHealthResponse] = {
    processResponse(client.execute(clusterHealthDefinition))
  }

  /**
    * Creates a Source (akka stream) from the result of the search
    */
  def source(searchDefinition: SearchDefinition): Source[SearchHit, NotUsed] = Source.fromPublisher(client.publisher(searchDefinition))

  //    private lazy val sinkListener = new ResponseListener[T] {
  //      override def onAck(resp: BulkResponseItem, original: T): Unit
  //      def onFailure(resp: BulkResponseItem, original: T): Unit = ()
  //      override def onAck(resp: RichBulkItemResponse): Unit = ()
  //      override def onFailure(resp: RichBulkItemResponse): Unit = {
  //        logger.warn(s"Document index failure ${resp.id}: ${resp.failureMessage}")
  //      }
  //    }

  /**
    * Create a Sink (akka stream) that create entity in ElasticSearch
    */
  def sink[T](implicit builder: RequestBuilder[T]): Sink[T, Future[Unit]] = {
    val sinkListener = new ResponseListener[T] {
      override def onAck(resp: BulkResponseItem, original: T): Unit = ()
      override def onFailure(resp: BulkResponseItem, original: T): Unit = {
        val errorMessage = resp.error.fold("unknown")(_.reason)
        logger.warn(s"Document index failure ${resp.id}: $errorMessage")
      }
    }

    val end = Promise[Unit]
    val complete = () ⇒ {
      if (!end.isCompleted)
        end.success(())
      ()
    }
    val failure = (t: Throwable) ⇒ {
      end.failure(t)
      ()
    }
    Sink.fromSubscriber(client.subscriber[T](failureWait = 1.second, maxAttempts = 10, listener = sinkListener, completionFn = complete, errorFn = failure))
      .mapMaterializedValue { _ ⇒ end.future }
  }

  /**
    * Name of the index, suffixed by the current version
    */
  val indexName: String = baseIndexName + "_" + version

  /**
    * return a new instance of DBConfiguration that points to the previous version of the index schema
    */
  def previousVersion: DBConfiguration = new DBConfiguration(searchHost, searchCluster, baseIndexName, xpackUsername, xpackPassword, lifecycle, version - 1, ec, actorSystem)
}
