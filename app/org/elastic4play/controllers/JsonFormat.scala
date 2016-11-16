package org.elastic4play.controllers

import java.io.File
import java.nio.file.{ Path, Paths }

import play.api.libs.json.{ Format, JsError, JsObject, JsString, JsSuccess }
import play.api.libs.json.{ Reads, Writes }
import play.api.libs.json.JsValue.jsValueToJsLookup
import play.api.libs.json.Json
import play.api.libs.json.Json.toJsFieldJsValueWrapper
import play.api.libs.json.JsValue

object JsonFormat {

  val fileReads = Reads[File] { json ⇒ json.validate[String].map(filepath ⇒ new File(filepath)) }
  val fileWrites = Writes[File]((file: File) ⇒ JsString(file.getAbsolutePath))
  implicit val fileFormat = Format[File](fileReads, fileWrites)

  val pathReads = Reads[Path] { json ⇒ json.validate[String].map(filepath ⇒ Paths.get(filepath)) }
  val pathWrites = Writes[Path]((path: Path) ⇒ JsString(path.toString))
  implicit val pathFormat = Format[Path](pathReads, pathWrites)

  val fileInputValueWrites = Writes[FileInputValue] { (fiv: FileInputValue) ⇒ fiv.jsonValue + ("type" → JsString("FileInputValue")) }
  val stringInputValueReads = Reads[StringInputValue] { json ⇒ (json \ "value").validate[Seq[String]].map(s ⇒ StringInputValue(s)) }
  val jsonInputValueReads = Reads[JsonInputValue] { json ⇒ (json \ "value").validate[JsValue].map(v ⇒ JsonInputValue(v)) }
  val fileInputValueReads = Reads[FileInputValue] { json ⇒
    for {
      name ← (json \ "name").validate[String]
      filepath ← (json \ "filepath").validate[Path]
      contentType ← (json \ "contentType").validate[String]
    } yield FileInputValue(name, filepath, contentType)
  }

  val inputValueWrites = Writes[InputValue]((value: InputValue) ⇒ value match {
    case v: StringInputValue ⇒ Json.obj("type" → "StringInputValue", "value" → v.jsonValue)
    case v: JsonInputValue   ⇒ Json.obj("type" → "JsonInputValue", "value" → v.jsonValue)
    case v: FileInputValue   ⇒ Json.obj("type" → "FileInputValue", "value" → v.jsonValue)
    case NullInputValue      ⇒ Json.obj("type" → "NullInputValue")
  })

  val inputValueReads = Reads { json ⇒
    (json \ "type").validate[String].flatMap {
      case "StringInputValue" ⇒ (json \ "value").validate(stringInputValueReads)
      case "JsonInputValue"   ⇒ (json \ "value").validate(jsonInputValueReads)
      case "FileInputValue"   ⇒ (json \ "value").validate(fileInputValueReads)
      case "NullInputValue"   ⇒ new JsSuccess(NullInputValue)
    }
  }

  implicit val fileInputValueFormat = Format[FileInputValue](fileInputValueReads, fileInputValueWrites)
  implicit val inputValueFormat = Format[InputValue](inputValueReads, inputValueWrites)

  implicit val fieldsReader = Reads {
    case json: JsObject ⇒ JsSuccess(Fields(json))
    case _              ⇒ JsError("Expecting JSON object body")
  }

}