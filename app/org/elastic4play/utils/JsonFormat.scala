package org.elastic4play.utils

import play.api.libs.json.{ Format, JsString, Reads, Writes }

object JsonFormat {
  val hashReads = Reads(json => json.validate[String].map(h => Hash(h)))
  val hashWrites = Writes[Hash](h => JsString(h.toString))
  implicit val hashFormat = Format(hashReads, hashWrites)
}