package common

import scala.concurrent.duration._
import scala.reflect.ClassTag

import play.api.cache.SyncCacheApi

object FakeCache extends SyncCacheApi {
  def set(key: String, value: Any, expiration: Duration = Duration.Inf): Unit = ()
  def remove(key: String): Unit = ()
  def getOrElseUpdate[A: ClassTag](key: String, expiration: Duration = Duration.Inf)(orElse: ⇒ A): A = orElse
  def get[T: ClassTag](key: String): Option[T] = None
}