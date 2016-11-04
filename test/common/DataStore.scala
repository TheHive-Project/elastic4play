//package common
//
//import java.io.{ File, InputStream }
//
//import scala.concurrent.Future
//
//import play.api.libs.json.JsObject
//
//import models.DataStoreApi
//import org.elastic4play.utils.Hash
//
//object FakeDataStore extends DataStoreApi {
//  def save(file: File): Future[(String, Seq[Hash])] = Future.successful(("blah", Nil))
//  def load(hash: String): InputStream = null
//  def getSize(hash: String): Long = 0
//  def getMetadata(hash: String, fileName: String): JsObject = JsObject(Nil)
//  def getMetadata(stream: InputStream, hash: String, fileName: String): JsObject = JsObject(Nil)
//}