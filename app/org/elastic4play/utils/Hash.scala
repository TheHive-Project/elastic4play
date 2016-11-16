package org.elastic4play.utils

import java.nio.charset.Charset
import java.nio.file.{ Path, Paths }
import java.security.MessageDigest

import scala.concurrent.{ ExecutionContext, Future }

import akka.stream.Materializer
import akka.stream.scaladsl.{ FileIO, Source }
import akka.util.ByteString

import play.api.libs.json.JsValue

// TODO use play.api.libs.Codecs

case class Hasher(algorithms: String*) {

  def fromPath(path: Path)(implicit mat: Materializer, ec: ExecutionContext): Future[Seq[Hash]] = {
    fromSource(FileIO.fromPath(path))
  }

  def fromSource(source: Source[ByteString, Any])(implicit mat: Materializer, ec: ExecutionContext): Future[Seq[Hash]] = {
    val mds = algorithms.map(algo ⇒ MessageDigest.getInstance(algo))
    source
      .runForeach { bs ⇒ mds.foreach(md ⇒ md.update(bs.toByteBuffer)) }
      .map { _ ⇒ mds.map(md ⇒ Hash(md.digest())) }
  }

  def fromString(data: String): Seq[Hash] = {
    val mds = algorithms.map(algo ⇒ MessageDigest.getInstance(algo))
    mds.map(md ⇒ Hash(md.digest(data.getBytes(Charset.forName("UTF8")))))
  }
}

class MultiHash(algorithms: String)(implicit mat: Materializer, ec: ExecutionContext) {
  val md = MessageDigest.getInstance(algorithms)
  def addValue(value: JsValue): Unit = {
    md.update(0.asInstanceOf[Byte])
    md.update(value.toString.getBytes)
  }
  def addFile(filename: String): Future[Unit] = {
    md.update(0.asInstanceOf[Byte])
    FileIO.fromPath(Paths.get(filename))
      .runForeach(bs ⇒ md.update(bs.toByteBuffer))
      .map(_ ⇒ ())
  }
  def digest: Hash = Hash(md.digest())
}

case class Hash(data: Array[Byte]) {
  override def toString(): String = data.map(b ⇒ "%02x".format(b)).mkString
}
object Hash {
  def apply(s: String): Hash = Hash {
    s
      .grouped(2)
      .map { cc ⇒ (Character.digit(cc(0), 16) << 4 | Character.digit(cc(1), 16)).toByte }
      .toArray
  }
}