package org.elastic4play.services

import java.util.Date
import javax.inject.Singleton

import scala.util.Try

import play.api.Logger
import play.api.libs.json.JsObject
import play.api.mvc.{ RequestHeader, Result }

import akka.actor.{ ActorRef, actorRef2Scala }
import akka.event.{ ActorEventBus, SubchannelClassification }
import akka.util.Subclassification

import org.elastic4play.models.{ BaseEntity, HiveEnumeration }

trait EventMessage

object AuditableAction extends Enumeration with HiveEnumeration {
  type Type = Value
  val Update, Creation, Delete, Get = Value
}

case class RequestProcessStart(request: RequestHeader) extends EventMessage
case class RequestProcessEnd(request: RequestHeader, result: Try[Result]) extends EventMessage

case class AuditOperation(
    entity: BaseEntity,
    action: AuditableAction.Type,
    details: JsObject,
    authContext: AuthContext,
    date: Date = new Date()) extends EventMessage

@Singleton
class EventSrv extends ActorEventBus with SubchannelClassification {
  private[EventSrv] lazy val logger = Logger(getClass)
  override type Classifier = Class[_ <: EventMessage]
  override type Event = EventMessage

  override protected def classify(event: EventMessage): Classifier = event.getClass
  override protected def publish(event: EventMessage, subscriber: ActorRef): Unit = subscriber ! event

  implicit protected def subclassification: Subclassification[Classifier] = new Subclassification[Classifier] {
    def isEqual(x: Classifier, y: Classifier): Boolean = x == y
    def isSubclass(x: Classifier, y: Classifier): Boolean = y.isAssignableFrom(x)
  }
}

