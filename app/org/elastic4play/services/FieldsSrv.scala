package org.elastic4play.services

import org.elastic4play.controllers.Fields
import org.elastic4play.controllers.JsonFormat.inputValueFormat
import org.elastic4play.models.BaseModelDef
import org.elastic4play.{ AttributeCheckingError, UnknownAttributeError }
import org.scalactic.Accumulation.convertGenTraversableOnceToValidatable
import org.scalactic._
import play.api.Logger
import play.api.libs.json._

class FieldsSrv {
  lazy val logger = Logger(getClass)
  def parse(fields: Fields, model: BaseModelDef): JsObject Or AttributeCheckingError = {
    fields
      .map {
        case (name, value) ⇒
          val names = name.split("\\.")
          (name, names, value, model.formAttributes.get(names.head))
      }
      .validatedBy {
        case (name, _, value, Some(_)) if value.jsonValue == JsNull || value.jsonValue == JsArray(Nil) ⇒ Good(name → value.jsonValue)
        //        case (name, names, JsonInputValue(JsObject(fields)), Some(attr)) ⇒
        //          fields
        //            .validatedBy {
        //              case (key, value) ⇒
        //                val keys = key.split("\\.")
        //                attr.format.inputValueToJson(names.tail ++ keys.init, JsonInputValue(JsObject(Seq(keys.last → value))))
        //            }
        //            .transform(v ⇒ Good(name → JsObject(fields)), es ⇒ Bad(es.map(e ⇒ e.withName(model.name + "." + name))))
        case (name, names, value, Some(attr)) ⇒ attr.format.inputValueToJson(names.tail, value).transform(v ⇒ Good(name → v), es ⇒ Bad(es.map(e ⇒ e.withName(model.name + "." + name))))
        case (_, names, value, None) ⇒ Bad(One(UnknownAttributeError(model.name + "." + names.mkString("."), Json.toJson(value))))
      }
      .transform(
        attrs ⇒ Good(JsObject(attrs.toSeq)),
        errors ⇒ Bad(AttributeCheckingError(model.name, errors.toSeq)))
  }
}