package org.elastic4play.services

import java.util.Date
import javax.inject.{ Inject, Singleton }

import scala.util.Try

import play.api.Logger
import play.api.libs.json.JsObject
import play.api.mvc.{ RequestHeader, Result }

import akka.actor.{ Actor, ActorRef, ActorSystem, Props, actorRef2Scala }
import akka.cluster.pubsub.DistributedPubSubMediator.{ Publish, Subscribe }
import akka.cluster.pubsub.DistributedPubSub
import akka.event.{ ActorEventBus, EventBus, SubchannelClassification }
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

trait EventSrv {
  def publish(event: EventMessage): Unit
  def subscribe(subscriber: ActorRef, classifier: Class[_ <: EventMessage]): Boolean
  def unsubscribe(subscriber: ActorRef, from: Class[_ <: EventMessage]): Boolean
  def unsubscribe(subscriber: ActorRef): Unit
}

class DistributedEventActor(localEventSrv: LocalEventSrv) extends Actor {
  override def receive: Receive = {
    case message: EventMessage ⇒ localEventSrv.publish(message)
  }
}

@Singleton
class DistributedEventSrv @Inject() (
    system: ActorSystem,
    localEventSrv: LocalEventSrv) extends EventBus with EventSrv {
  type Event = EventMessage
  type Classifier = Class[_ <: EventMessage]
  type Subscriber = ActorRef

  private val mediator = DistributedPubSub(system).mediator

  private val eventActor = system.actorOf(Props(classOf[DistributedEventActor], localEventSrv), "DistributedEventActor")
  mediator ! Subscribe("stream", eventActor)

  def publish(event: Event): Unit = {
    mediator ! Publish("stream", event)
  }

  def subscribe(subscriber: Subscriber, classifier: Classifier): Boolean = {
    localEventSrv.subscribe(subscriber, classifier)
  }

  def unsubscribe(subscriber: Subscriber, from: Classifier): Boolean = {
    localEventSrv.unsubscribe(subscriber, from)
  }

  def unsubscribe(subscriber: Subscriber): Unit = {
    localEventSrv.unsubscribe(subscriber)
  }
}

@Singleton
class LocalEventSrv extends ActorEventBus with SubchannelClassification with EventSrv {
  private[LocalEventSrv] lazy val logger = Logger(getClass)
  override type Classifier = Class[_ <: EventMessage]
  override type Event = EventMessage

  override protected def classify(event: EventMessage): Classifier = event.getClass
  override protected def publish(event: EventMessage, subscriber: ActorRef): Unit = subscriber ! event

  implicit protected def subclassification: Subclassification[Classifier] = new Subclassification[Classifier] {
    def isEqual(x: Classifier, y: Classifier): Boolean = x == y
    def isSubclass(x: Classifier, y: Classifier): Boolean = y.isAssignableFrom(x)
  }
}
