package org.elastic4play.services

import scala.collection.JavaConversions._

import com.typesafe.config.ConfigValueType._
import com.typesafe.config.{ ConfigObject, ConfigList, ConfigValue }

import play.api.libs.json._
import play.api.Configuration

import com.sksamuel.elastic4s.QueryDefinition

import org.elastic4play.models.BaseEntity
import org.elastic4play.models.JsonFormat._
import org.elastic4play.utils.JsonFormat.hashFormat
import org.elastic4play.utils.Hash
import QueryDSL._
import play.api.Logger

object JsonFormat {
  lazy val log = Logger(getClass)

  val attachmentWrites: Writes[Attachment] = Writes((attachment: Attachment) ⇒
    Json.obj(
      "name" → attachment.name,
      "hashes" → attachment.hashes,
      "size" → attachment.size,
      "contentType" → attachment.contentType,
      "id" → attachment.id
    ))

  val attachmentReads: Reads[Attachment] = Reads((json: JsValue) ⇒
    for {
      name ← (json \ "name").validate[String]
      hashes ← (json \ "hashes").validate[Seq[Hash]]
      size ← (json \ "size").validate[Long]
      contentType ← (json \ "contentType").validate[String]
      id ← (json \ "id").validate[String]
    } yield Attachment(name, hashes, size, contentType, id))

  implicit val attachmentFormat = Format(attachmentReads, attachmentWrites)

  implicit def roleFormat = enumFormat(Role)

  implicit val configValueWrites: Writes[ConfigValue] = Writes((value: ConfigValue) ⇒ value match {
    case v: ConfigObject             ⇒ configWrites.writes(Configuration(v.toConfig()))
    case v: ConfigList               ⇒ JsArray(v.toSeq.map(x ⇒ configValueWrites.writes(x)))
    case v if v.valueType == NUMBER  ⇒ JsNumber(BigDecimal(v.unwrapped.asInstanceOf[java.lang.Number].toString))
    case v if v.valueType == BOOLEAN ⇒ JsBoolean(v.unwrapped.asInstanceOf[Boolean])
    case v if v.valueType == NULL    ⇒ JsNull
    case v if v.valueType == STRING  ⇒ JsString(v.unwrapped.asInstanceOf[String])
  })

  implicit val configWrites = OWrites { (cfg: Configuration) ⇒
    JsObject(cfg.subKeys.map(key ⇒ key → configValueWrites.writes(cfg.underlying.getValue(key))).toSeq)
  }

  //def jsonGet[A](json: JsValue, name:  String)(implicit reads: Reads[A]) = (json \ name).as[A]

  object JsObj {
    def unapply(v: JsValue): Option[Seq[(String, JsValue)]] = v match {
      case JsObject(f) ⇒ Some(f.toSeq)
      case _           ⇒ None
    }
  }

  object JsObjOne {
    def unapply(v: JsValue): Option[(String, JsValue)] = v match {
      case JsObject(f) if f.size == 1 ⇒ f.toSeq.headOption
      case _                          ⇒ None
    }
  }

  object JsVal {
    def unapply(v: JsValue): Option[Any] = v match {
      case JsString(s)  ⇒ Some(s)
      case JsBoolean(b) ⇒ Some(b)
      case JsNumber(i)  ⇒ Some(i)
      case _            ⇒ None
    }
  }

  object JsRange {
    def unapply(v: JsValue): Option[(String, Any, Any)] =
      for {
        field ← (v \ "_field").asOpt[String]
        jsFrom ← (v \ "_from").asOpt[JsValue]
        from ← JsVal.unapply(jsFrom)
        jsTo ← (v \ "_to").asOpt[JsValue]
        to ← JsVal.unapply(jsTo)
      } yield (field, from, to)
  }

  object JsParent {
    def unapply(v: JsValue): Option[(String, QueryDef)] =
      for {
        t ← (v \ "_type").asOpt[String]
        q ← (v \ "_query").asOpt[QueryDef]
      } yield (t, q)
  }

  object JsField {
    def unapply(v: JsValue): Option[(String, Any)] =
      for {
        f ← (v \ "_field").asOpt[String]
        maybeValue ← (v \ "_value").asOpt[JsValue]
        value ← JsVal.unapply(maybeValue)
      } yield (f, value)
  }

  object JsFieldIn {
    def unapply(v: JsValue): Option[(String, Seq[String])] =
      for {
        f ← (v \ "_field").asOpt[String]
        values ← (v \ "_values").asOpt[Seq[String]]
      } yield f → values
  }

  implicit val queryReads: Reads[QueryDef] = {
    Reads { (json: JsValue) ⇒
      json match {
        case JsObjOne(("_and", JsArray(v)))            ⇒ JsSuccess(and(v.map(_.as[QueryDef]): _*))
        case JsObjOne(("_or", JsArray(v)))             ⇒ JsSuccess(or(v.map(_.as[QueryDef]): _*))
        case JsObjOne(("_contains", JsString(v)))      ⇒ JsSuccess(contains(v))
        case JsObjOne(("_not", v: JsObject))           ⇒ JsSuccess(not(v.as[QueryDef]))
        case JsObjOne(("_any", _))                     ⇒ JsSuccess(any)
        case j: JsObject if j.fields.isEmpty           ⇒ JsSuccess(any)
        case JsObjOne(("_gt", JsObjOne(n, JsVal(v))))  ⇒ JsSuccess(n ~> v)
        case JsObjOne(("_gte", JsObjOne(n, JsVal(v)))) ⇒ JsSuccess(n ~>= v)
        case JsObjOne(("_lt", JsObjOne(n, JsVal(v))))  ⇒ JsSuccess(n ~< v)
        case JsObjOne(("_lte", JsObjOne(n, JsVal(v)))) ⇒ JsSuccess(n ~<= v)
        case JsObjOne(("_between", JsRange(n, f, t)))  ⇒ JsSuccess(n ~<> (f → t))
        case JsObjOne(("_parent", JsParent(p, q)))     ⇒ JsSuccess(parent(p, q))
        case JsObjOne(("_id", JsString(id)))           ⇒ JsSuccess(withId(id))
        case JsField(field, value)                     ⇒ JsSuccess(field ~= value)
        case JsObjOne(("_child", JsParent(p, q)))      ⇒ JsSuccess(child(p, q))
        case JsObjOne(("_string", JsString(s)))        ⇒ JsSuccess(string(s))
        case JsObjOne(("_in", JsFieldIn(f, v)))        ⇒ JsSuccess(f in (v: _*))
        case JsObjOne(("_type", JsString(v)))          ⇒ JsSuccess(ofType(v))
        case JsObjOne((n, JsVal(v)))                   ⇒
          if (n.startsWith("_")) log.warn(s"""Potentially invalid search query : {"$n": "$v"}"""); JsSuccess(n ~= v)
        case other                                     ⇒ JsError(s"Invalid query: unexpected $other")
      }
    }
  }

  implicit val aggReads: Reads[Agg] = Reads { (json: JsValue) ⇒
    (json \ "_agg").as[String] match {
      case "avg"   ⇒ JsSuccess(selectAvg((json \ "_field").as[String]))
      case "min"   ⇒ JsSuccess(selectMin((json \ "_field").as[String]))
      case "max"   ⇒ JsSuccess(selectMax((json \ "_field").as[String]))
      case "sum"   ⇒ JsSuccess(selectSum((json \ "_field").as[String]))
      case "count" ⇒ JsSuccess(selectCount)
      case "time" ⇒
        val fields = (json \ "_fields").as[Seq[String]]
        val interval = (json \ "_interval").as[String]
        val selectables = (json \ "_select").as[Seq[Agg]]
        JsSuccess(groupByTime(fields, interval, selectables: _*))
      case "field" ⇒
        val field = (json \ "_field").as[String]
        val size = (json \ "_size").asOpt[Int].getOrElse(10)
        val order = (json \ "_order").asOpt[Seq[String]].getOrElse(Nil)
        val selectables = (json \ "_select").as[Seq[Agg]]
        JsSuccess(groupByField(field, size, order, selectables: _*))
      case "category" ⇒
        val categories = (json \ "_categories").as[Map[String, QueryDef]]
        val selectables = (json \ "_select").as[Seq[Agg]]
        JsSuccess(groupByCaterogy(categories, selectables: _*))
    }
  }

  implicit val authContextWrites = Writes[AuthContext]((authContext: AuthContext) ⇒ Json.obj(
    "id" → authContext.userId,
    "name" → authContext.userName,
    "roles" → authContext.roles
  ))

  implicit val auditableActionFormat = enumFormat(AuditableAction)

  implicit val AuditOperationWrites = Json.writes[AuditOperation]
}