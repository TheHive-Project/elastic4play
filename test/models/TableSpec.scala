//package models
//
//import play.api.libs.json.{ JsNumber, JsString }
//import play.api.test.{ PlaySpecification, FakeRequest }
//
//import org.specs2.runner.JUnitRunner
//import org.junit.runner.RunWith
//import org.specs2.mock.Mockito
//
//import com.sksamuel.elastic4s.ElasticDsl.RichFuture
//
//import common.{ Table, Fields, StringInputValue, JsonInputValue, FileInputValue, NullInputValue, AttributeCheckingError }
//import common.Table.attribute
//import org.elastic4play.utils.AttributeFormat._
//import org.elastic4play.utils.{ AttributeOption => O }
//
//@RunWith(classOf[JUnitRunner])
//class TableSpec extends PlaySpecification with Mockito {
//  "Table" should {
//    "accept creation of entity with string attributes" in {
//      Table(attribute("a", stringFmt, O.required),
//        attribute("b", stringFmt, O.required))
//        .checkForCreation(Fields.empty + ("a", "plop") + ("b", JsString("toto")))
//        .await must be equalTo true
//    }
//
//    "accept creation of entity with integer attributes" in {
//      Table(attribute("a", numberFmt, O.required),
//        attribute("b", numberFmt, O.required))
//        .checkForCreation(Fields.empty + ("a", "456") + ("b", JsNumber(44)))
//        .await must be equalTo true
//    }
//
//    "refuse creation if sending integer instead of string" in {
//      Table(attribute("a", stringFmt))
//        .checkForCreation(Fields.empty + ("a", JsNumber(3)))
//        .await must throwAn[AttributeCheckingError]
//    }
//
//    "accept creation if sending integer as string for integer attribute" in {
//      Table(attribute("a", numberFmt, O.required))
//        .checkForCreation(Fields.empty + ("a", "42"))
//        .await must be equalTo true
//    }
//
//    "refuse creation if a required attribute is missing" in {
//      Table(attribute("a", stringFmt, O.required),
//        attribute("b", numberFmt, O.required))
//        .checkForCreation(Fields.empty + ("a", "value"))
//        .await must throwAn[AttributeCheckingError]
//    }
//
//    "refuse creation if sending multiple values for single valued attribute" in {
//      Table(attribute("a", numberFmt))
//        .checkForCreation(Fields.empty + ("a", StringInputValue(Seq("1", "2", "42"))))
//        .await must throwAn[AttributeCheckingError]
//    }
//
//    "accept creation of entity with multivalued integer attribute" in {
//      Table(attribute("a", numberFmt, O.multi))
//        .checkForCreation(Fields.empty + ("a", StringInputValue(Seq("1", "2", "42"))))
//        .await must be equalTo true
//    }
//  }
//}