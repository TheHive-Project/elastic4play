package common

import scala.concurrent.duration._
import scala.reflect.ClassTag

import play.api.cache.CacheApi

object FakeCache extends CacheApi {
  def set(key: String, value: Any, expiration: Duration = Duration.Inf) = ()
  def remove(key: String) = ()
  def getOrElse[A: ClassTag](key: String, expiration: Duration = Duration.Inf)(orElse: => A): A = orElse
  def get[T: ClassTag](key: String): Option[T] = None
}