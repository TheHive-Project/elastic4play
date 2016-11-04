package org.elastic4play.utils

import java.rmi.dgc.VMID
import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.implicitNotFound

import play.api.mvc.RequestHeader
import org.elastic4play.services.AuthContext

object Instance {
  val id = (new VMID).toString
  val counter = new AtomicInteger(0)
  def getRequestId(request: RequestHeader) = s"$id:${request.id}"
  def getInternalId = s"$id::${counter.incrementAndGet}"
}