package mario.tfm.postgresql
import java.sql.Connection
import java.util.UUID
import java.util.logging.Logger

import play.api.libs.json.JsObject
import mario.tfm.postgresql.PostgresTools._

import scala.collection.mutable.ListBuffer

object DataToolsPostgres {

  val logger = Logger.getLogger(this.getClass.getSimpleName)

  def assignUuid (): UUID = {
    val uuid = UUID.randomUUID()
    uuid
  }

  // Mirar que en este método he puesto el Json de circe y estaba JsObject entonces tengo que cambiar todo
  def dataToInsertEntity(jsonobject: JsObject)(implicit connection: Connection)
  = {
    var id = ""
    var column_names = ListBuffer[String]()
    var values = ListBuffer[String]()
    //logger.info(s"Json Object fields: ${jsonobject.fields}")
    for (j <- jsonobject.fields) {
      if (j._1 == "id") {
        id = j._2.as[String]
      }

      if (!j._2.isInstanceOf[JsObject]) {
        column_names.append(j._1)
        values.append(s"'${j._2.toString().replace("\"", "")}'")
      }
    }
    insertPostgres("Entity", column_names, values)


    for (j <- jsonobject.fields){
      if (j._2.isInstanceOf[JsObject]) {
        //logger.info(s"Esto es instancia de objeto: ${j._1} , ${j._2}")
        val uuid = assignUuid().toString
        values.append("'" + uuid + "'")

        //val usefuldata =
        dataToInsertPropOrRel(j._2.as[JsObject], j._1, id)

      }
    }

  }


  def dataToInsertPropOrRel(jsonobject: JsObject, category: String, father: String)(implicit connection: Connection)
  = {
    var table_name = ""
    val column_names = ListBuffer[String]()
    val values = ListBuffer[String]()
    column_names.append("EntityId")
    values.append("'" + father + "'")
    column_names.append("category")

    values.append("'" + category + "'")

    //logger.info(s"Json Object fields: ${jsonobject.fields}")
    for (j <- jsonobject.fields) {
      //logger.info(s"Mensaje: ${j.toString()}")
      if (j._1 == "type") {
        table_name = j._2.as[String]
      }
      else{
        if (!j._2.isInstanceOf[JsObject]) {
          column_names.append(j._1)
          values.append(s"'${j._2.toString().replace("\"", "")}'")
        }
      }

    }
    val uuid = assignUuid().toString
    column_names.append(table_name + "Id")
    values.append("'" + uuid + "'")

    if (table_name == "GeoProperty") {
      logger.info("HA ENTRADO EN GEOPROPERTY")
      column_names.append("value")

      val value = (jsonobject \ "value").as[JsObject]
      values.append("'"+ value + "'")

    }

    insertPostgres(table_name, column_names, values)


    for (j <- jsonobject.fields){
      if (j._2.isInstanceOf[JsObject]) {

        dataToInsertPropOrRel_2(j._2.as[JsObject], j._1, uuid, table_name)

      }
    }
  }

  def dataToInsertPropOrRel_2(jsonobject: JsObject, category: String, father: String, fathertype: String)
                             (implicit connection: Connection)
  = {
    var table_name = ""
    var id = ""
    var column_names = ListBuffer[String]()
    var values = ListBuffer[String]()
    column_names.append("category")
    values.append("'" + category + "'")

    //logger.info(s"Json Object fields: ${jsonobject.fields}")
    for (j <- jsonobject.fields) {
      //logger.info(s"Mensaje: ${j.toString()}")
      if (j._1 == "type") {
        val kind = j._2.as[String]
        id = j._2.as[String] + "Id"

        if (kind == fathertype) {
          table_name = j._2.as[String] + "_2"

          if (table_name == "Property_2") {
            column_names.append("FatherPropertyId")
          }
          else {
            column_names.append("FatherRelationshipId")
          }
          values.append("'" + father + "'")

        }

        else {
          table_name = "Property_Relationship"

          if (fathertype == "Property"){
            column_names.append("PropertyId")
            values.append("'" + father + "'")
          }
          else {
            column_names.append("RelationshipId")
            values.append("'" + father + "'")
          }
        }
      }

      else{
        if (!j._2.isInstanceOf[JsObject]) {
          column_names.append(j._1)
          values.append(s"'${j._2.toString().replace("\"", "")}'")
        }
      }

    }

    if (table_name == "Property_2" || table_name == "Relationship_2"){
      val uuid = assignUuid().toString
      column_names.append(id)
      values.append("'" + uuid + "'")
    }


    insertPostgres(table_name, column_names, values)

  }

  def sendToPostgres(data: Vector[JsObject])(implicit connection: Connection) = {
    //antes venía como parámetro jsonObject y de ahí sacaban esto
    //val data = (jsonObject \ "data").as[JsArray].value
    for (i <- data){
      println("Objetos de data " + i)
      dataToInsertEntity(i)
    }
  }

}
