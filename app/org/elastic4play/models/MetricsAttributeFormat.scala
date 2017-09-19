package org.elastic4play.models

import play.api.libs.json._

import com.sksamuel.elastic4s.ElasticDsl.{ longField, nestedField }
import com.sksamuel.elastic4s.mappings.NestedFieldDefinition
import org.scalactic.Accumulation._
import org.scalactic._

import org.elastic4play.AttributeError
import org.elastic4play.controllers.{ InputValue, JsonInputValue }
import org.elastic4play.services.DBLists

class MetricsAttributeFormat extends AttributeFormat[JsValue]("metrics") {
  override def checkJson(subNames: Seq[String], value: JsValue): Or[JsValue, Every[AttributeError]] = fromInputValue(subNames, JsonInputValue(value))

  override def fromInputValue(subNames: Seq[String], value: InputValue): JsValue Or Every[AttributeError] = {
    if (subNames.isEmpty) {
      value match {
        case JsonInputValue(v: JsObject) ⇒
          v.fields
            .validatedBy {
              case (_, _: JsNumber) ⇒ Good(())
              case (_, JsNull)      ⇒ Good(())
              case _                ⇒ formatError(value)
            }
            .map(_ ⇒ v)
        case _ ⇒ formatError(value)
      }
    }
    else {
      OptionalAttributeFormat(NumberAttributeFormat).inputValueToJson(subNames.tail, value) //.map(v => JsObject(Seq(subNames.head -> v)))
    }
  }

  override def elasticType(attributeName: String): NestedFieldDefinition = nestedField(attributeName).fields(Seq(longField("_default_")))

  override def definition(dblists: DBLists, attribute: Attribute[JsValue]): Seq[AttributeDefinition] = {
    dblists("case_metrics").cachedItems.flatMap { item ⇒
      val itemObj = item.mapTo[JsObject]
      for {
        fieldName ← (itemObj \ "name").asOpt[String]
        description ← (itemObj \ "description").asOpt[String]
      } yield AttributeDefinition(
        s"${attribute.name}.$fieldName",
        "number",
        description,
        Nil,
        Nil)
    }
  }
}

object MetricsAttributeFormat extends MetricsAttributeFormat