package common

import scala.util.Random

import play.api.libs.json.{JsBoolean, JsNumber, JsObject, JsString}

object Fabricator {
  def string(prefix: String = "", size: Int = 10) = prefix + Random.alphanumeric.take(size).mkString
  def int                                         = Random.nextInt
  def boolean                                     = Random.nextBoolean()
  def long                                        = Random.nextLong

  def jsValue = int % 4 match {
    case 0 => JsNumber(long)
    case 1 => JsBoolean(boolean)
    case _ => JsString(string())
  }

  def jsObject(maxSize: Int = 10) = {
    val fields = Seq.fill(int % maxSize)(string() -> jsValue)
    JsObject(fields)
  }
}
