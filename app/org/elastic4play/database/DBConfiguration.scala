package org.elastic4play.database

import javax.inject.{ Inject, Named, Singleton }
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ ExecutionContext, Future, Promise }

import play.api.inject.ApplicationLifecycle
import play.api.{ Configuration, Logger }

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Sink, Source }
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.admin.IndexExistsDefinition
import com.sksamuel.elastic4s.bulk.RichBulkItemResponse
import com.sksamuel.elastic4s.cluster.{ ClusterHealthDefinition, ClusterStatsDefinition }
import com.sksamuel.elastic4s.delete.DeleteByIdDefinition
import com.sksamuel.elastic4s.get.{ GetDefinition, RichGetResponse }
import com.sksamuel.elastic4s.index.RichIndexResponse
import com.sksamuel.elastic4s.indexes.{ CreateIndexDefinition, IndexDefinition }
import com.sksamuel.elastic4s.searches._
import com.sksamuel.elastic4s.streams.ReactiveElastic.ReactiveElastic
import com.sksamuel.elastic4s.streams.{ RequestBuilder, ResponseListener }
import com.sksamuel.elastic4s.update.{ RichUpdateResponse, UpdateDefinition }
import com.sksamuel.elastic4s.{ ElasticsearchClientUri, TcpClient }
import com.sksamuel.elastic4s.xpack.security.XPackElasticClient
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.common.settings.Settings
import com.floragunn.searchguard.ssl.SearchGuardSSLPlugin
import com.floragunn.searchguard.ssl.util.SSLConfigConstants

import org.elastic4play.Timed

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
    xpackUsername: Option[String],
    xpackPassword: Option[String],
    xpackSSL: Boolean,
    xpackCAPath: Option[String],
    xpackCertificatePath: Option[String],
    xpackKeyPath: Option[String],
    sgKeystorePath: Option[String],
    sgTruststorePath: Option[String],
    sgKeystorePassword: Option[String],
    sgTruststorePassword: Option[String],
    sgHostVerification: Boolean,
    sgHostVerificationResolveHostname: Boolean,
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
      configuration.getOptional[Boolean]("search.ssl.enabled").getOrElse(false),
      configuration.getOptional[String]("search.ssl.ca"),
      configuration.getOptional[String]("search.ssl.certificate"),
      configuration.getOptional[String]("search.ssl.key"),
      configuration.getOptional[String]("search.guard.keyStore.path"),
      configuration.getOptional[String]("search.guard.trustStore.path"),
      configuration.getOptional[String]("search.guard.keyStore.password"),
      configuration.getOptional[String]("search.guard.trustStore.password"),
      configuration.getOptional[Boolean]("search.guard.hostVerification").getOrElse(false),
      configuration.getOptional[Boolean]("search.guard.hostVerificationResolveHostname").getOrElse(false),
      lifecycle,
      version,
      ec,
      actorSystem)
  }

  private[DBConfiguration] lazy val logger = Logger(getClass)

  private def xpackConnect(uri: ElasticsearchClientUri, settings: Settings.Builder): Option[TcpClient] = {
    for {
      username ← xpackUsername
      if username.nonEmpty
      password ← xpackPassword
      if password.nonEmpty
    } yield {
      settings.put("xpack.security.user", s"$username:$password")
      if (xpackSSL) {
        settings.put("xpack.security.transport.ssl.enabled", "true")
        xpackCAPath.foreach(ca ⇒ settings.put("xpack.ssl.certificate_authorities", ca))
        xpackCertificatePath.foreach(cp ⇒ settings.put("xpack.ssl.certificate", cp))
        xpackKeyPath.foreach(k ⇒ settings.put("xpack.ssl.key", k))
      }
      XPackElasticClient(settings.build(), uri)
    }
  }

  private def sgConnect(uri: ElasticsearchClientUri, settings: Settings.Builder): Option[TcpClient] = {
    for {
      keystorePath ← sgKeystorePath
      truststorePath ← sgTruststorePath
      keystorePassword ← sgKeystorePassword
      truststorePassword ← sgTruststorePassword
    } yield {
      settings.put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_FILEPATH, keystorePath)
      settings.put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_FILEPATH, truststorePath)
      settings.put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_PASSWORD, keystorePassword)
      settings.put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_PASSWORD, truststorePassword)
      settings.put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENFORCE_HOSTNAME_VERIFICATION, sgHostVerification)
      settings.put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENFORCE_HOSTNAME_VERIFICATION_RESOLVE_HOST_NAME, sgHostVerificationResolveHostname)
      TcpClient.transport(settings.build(), uri, classOf[SearchGuardSSLPlugin])
    }
  }

  private def connect(): TcpClient = {
    val uri = ElasticsearchClientUri(s"elasticsearch://${searchHost.mkString(",")}")
    val settings = Settings.builder()
    settings.put("cluster.name", searchCluster)

    xpackConnect(uri, settings)
      .orElse(sgConnect(uri, settings))
      .getOrElse(TcpClient.transport(settings.build(), uri))
  }

  /**
    * Underlying ElasticSearch client
    */
  private[database] val client = connect()
  // when application close, close also ElasticSearch connection
  lifecycle.addStopHook { () ⇒ Future { client.close() } }

  @Timed("database.index")
  def execute(indexDefinition: IndexDefinition): Future[RichIndexResponse] = client.execute(indexDefinition)
  @Timed("database.search")
  def execute(searchDefinition: SearchDefinition): Future[RichSearchResponse] = client.execute(searchDefinition)
  @Timed("database.create")
  def execute(createIndexDefinition: CreateIndexDefinition): Future[CreateIndexResponse] = client.execute(createIndexDefinition)
  @Timed("database.update")
  def execute(updateDefinition: UpdateDefinition): Future[RichUpdateResponse] = client.execute(updateDefinition)
  @Timed("database.search_scroll")
  def execute(searchScrollDefinition: SearchScrollDefinition): Future[RichSearchResponse] = client.execute(searchScrollDefinition)
  @Timed("database.index_exists")
  def execute(indexExistsDefinition: IndexExistsDefinition): Future[IndicesExistsResponse] = client.execute(indexExistsDefinition)
  @Timed("database.delete")
  def execute(deleteByIdDefinition: DeleteByIdDefinition): Future[DeleteResponse] = client.execute(deleteByIdDefinition)
  @Timed("database.get")
  def execute(getDefinition: GetDefinition): Future[RichGetResponse] = client.execute(getDefinition)
  @Timed("database.clear_scroll")
  def execute(clearScrollDefinition: ClearScrollDefinition): Future[ClearScrollResult] = client.execute(clearScrollDefinition)
  @Timed("database.cluster_health")
  def execute(clusterHealthDefinition: ClusterHealthDefinition): Future[ClusterHealthResponse] = client.execute(clusterHealthDefinition)
  @Timed("database.cluster_stats")
  def execute(clusterStatsDefinition: ClusterStatsDefinition): Future[ClusterStatsResponse] = client.execute(clusterStatsDefinition)

  /**
    * Creates a Source (akka stream) from the result of the search
    */
  def source(searchDefinition: SearchDefinition): Source[RichSearchHit, NotUsed] = Source.fromPublisher(client.publisher(searchDefinition))

  private lazy val sinkListener = new ResponseListener {
    override def onAck(resp: RichBulkItemResponse): Unit = ()
    override def onFailure(resp: RichBulkItemResponse): Unit = {
      logger.warn(s"Document index failure ${resp.id}: ${resp.failureMessage}")
    }
  }

  /**
    * Create a Sink (akka stream) that create entity in ElasticSearch
    */
  def sink[T](implicit builder: RequestBuilder[T]): Sink[T, Future[Unit]] = {
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
  def previousVersion: DBConfiguration = new DBConfiguration(
    searchHost,
    searchCluster,
    baseIndexName,
    xpackUsername,
    xpackPassword,
    xpackSSL,
    xpackCAPath,
    xpackCertificatePath,
    xpackKeyPath,
    sgKeystorePath,
    sgTruststorePath,
    sgKeystorePassword,
    sgTruststorePassword,
    sgHostVerification,
    sgHostVerificationResolveHostname,
    lifecycle,
    version - 1,
    ec,
    actorSystem)
}
