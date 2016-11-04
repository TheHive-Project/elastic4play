//package common
//
//import java.util.Date
//import java.lang.reflect.Constructor
//
//import scala.concurrent.Future
//import scala.reflect.ClassTag
//
//import play.api.Configuration
//import play.api.libs.json.{ Json, JsString, JsObject }
//import play.api.libs.concurrent.Execution.Implicits._
//
//import org.specs2.mock.Mockito
//import org.mockito.{ Mockito => Mock }
//
//import org.elastic4play.utils.{ AttributeDesc, AttributeFormat, AttributeOption }
//import database.{ BaseTable, TableWithAttributes, Database }
//import models.{ Cases, Users, CaseStatus }
//
//case class Model[T <: BaseTable](implicit classTag: ClassTag[T]) extends Mockito {
//  val ctor = classTag.runtimeClass.getConstructors.head
//  val params = ctor.getParameterTypes.map(c => mock[Object](ClassTag(c).asInstanceOf[ClassTag[Object]]))
//  val tableInstance = ctor.newInstance(params: _*)
//  val table = spy[T](tableInstance.asInstanceOf[T])
//
//  table.tableName returns classTag.toString.toLowerCase
//
//  def getEntity(id: String, parent: Option[String], attributes: JsObject) = {
//    val entityInstance = table.read(id, parent, attributes)
//    val entity = spy(entityInstance)
//    doReturn(Future.successful(entity)).when(table).apply(id)
//    entity
//  }
//}
//
//case class Table(attributes: utils.AttributeDesc*) extends BaseTable("testTable")(Mock.mock(classOf[Database])) with TableWithAttributes {
//  override type ENTITY = TestEntity
//  val dataStore = FakeDataStore
//  def read(id: String, parentId: Option[String], attributes: JsObject) = new TestEntity(id, attributes)
//  val cache = FakeCache
//  case class TestEntity(id: String, attributes: JsObject) extends Entity
//
//  def checkForCreation(fields: Fields) = {
//    getJsonForm(fields)
//      .flatMap(v => checkAttributesForCreation(v))
//      .flatMap(v => attachmentHandler(v))
//      .map(_ => true)
//  }
//
//  def checkForUpdate(fields: Fields) = {
//    getJsonForm(fields)
//      .flatMap(v => checkAttributesForUpdate(v))
//      .map(_ => true)
//  }
//}
//object Table {
//  def attribute(name: String, format: AttributeFormat[_], options: AttributeOption.Type*) = AttributeDesc("testTable", name, format, options, None, "")
//}