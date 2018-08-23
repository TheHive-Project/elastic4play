package org.elastic4play

import java.nio.file.{ Files, Paths }
import java.security.KeyStore

import play.api.Logger
import play.core.ApplicationProvider
import play.core.server.ServerConfig
import play.server.api.SSLEngineProvider

import javax.net.ssl._

class ClientAuthSSLEngineProvider(serverConfig: ServerConfig, appProvider: ApplicationProvider) extends SSLEngineProvider {

  lazy val logger = Logger(getClass)
  private val config = serverConfig.configuration

  def readKeyManagers(): Array[KeyManager] = {
    val keyStorePath = Paths.get(config.get[String]("play.server.https.keyStore.path"))
    val keyStoreType = config.getOptional[String]("play.server.https.keyStore.type").getOrElse(KeyStore.getDefaultType)
    val keyStorePassword = config.getOptional[String]("play.server.https.keyStore.password").getOrElse("").toCharArray
    val keyInputStream = Files.newInputStream(keyStorePath)
    try {
      val keyStore = KeyStore.getInstance(keyStoreType)
      keyStore.load(keyInputStream, keyStorePassword)
      val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
      kmf.init(keyStore, keyStorePassword)
      kmf.getKeyManagers
    }
    finally {
      keyInputStream.close()
    }
  }

  def readTrustManagers(): Array[TrustManager] = {
    config.getOptional[String]("play.server.https.trustStore.path")
      .map { trustStorePath ⇒
        val keyStoreType = config.getOptional[String]("play.server.https.keyStore.type").getOrElse(KeyStore.getDefaultType)
        val trustStorePassword = config.getOptional[String]("play.server.https.trustStore.password").getOrElse("").toCharArray
        val trustInputStream = Files.newInputStream(Paths.get(trustStorePath))
        try {
          val keyStore = KeyStore.getInstance(keyStoreType)
          keyStore.load(trustInputStream, trustStorePassword)
          val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
          tmf.init(keyStore)
          tmf.getTrustManagers
        }
        finally {
          trustInputStream.close()
        }
      }
      .getOrElse(Array.empty)
  }

  def createSSLContext(applicationProvider: ApplicationProvider): SSLContext = {
    val keyManagers = readKeyManagers()
    val trustManagers = readTrustManagers()

    // Configure the SSL context to use TLS
    val sslContext = SSLContext.getInstance("TLS")
    sslContext.init(keyManagers, trustManagers, null)
    sslContext
  }

  override def createSSLEngine(): SSLEngine = {
    val sslContext = createSSLContext(appProvider)

    // Start off with a clone of the default SSL parameters...
    val sslParameters = sslContext.getDefaultSSLParameters

    // Tells the server to ignore client's cipher suite preference.
    // http://docs.oracle.com/javase/8/docs/technotes/guides/security/jsse/JSSERefGuide.html#cipher_suite_preference
    sslParameters.setUseCipherSuitesOrder(true)

    // http://docs.oracle.com/javase/8/docs/technotes/guides/security/jsse/JSSERefGuide.html#SSLParameters
    val wantClientAuth = config.getOptional[Boolean]("auth.method.pki").getOrElse(false)
    logger.debug(s"Client certificate authentication is ${if (wantClientAuth) "enable" else "disable"}")
    sslParameters.setWantClientAuth(wantClientAuth)

    // Clone and modify the default SSL parameters.
    val engine = sslContext.createSSLEngine
    engine.setSSLParameters(sslParameters)
    engine
  }
}
