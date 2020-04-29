package mario.tfm.postgresql
import java.sql.Connection
import java.util.UUID
import java.util.logging.Logger

import play.api.libs.json.JsObject
import mario.tfm.postgresql.SqlTools._

import scala.collection.mutable.ListBuffer

object DataTools {

  val logger = Logger.getLogger(this.getClass.getSimpleName)

  def assignUuid (): UUID = {
    val uuid = UUID.randomUUID()
    uuid
  }

  // Mirar que en este método he puesto el Json de circe y estaba JsObject entonces tengo que cambiar todo
  def dataToInsert(connection: Connection, jsonobject: JsObject): (String, ListBuffer[String], ListBuffer[String]) = {
    var table_name: String = ""
    var column_names =  ListBuffer[String]()
    var values = ListBuffer[String]()
    logger.info(s"Json Object fields: ${jsonobject.fields}")
    for (j <- jsonobject.fields){
      logger.info(s"Mensaje: ${j.toString()}")
      if (j._1 == "type"){
        logger.info(s"Type: ${j._1} , table_name ${j._2}")
        table_name = j._2.toString().replace("\"","")
      }
      else {
        column_names.append(j._1)
        if (j._2.isInstanceOf[JsObject]){
          logger.info(s"Esto es instancia de objeto: ${j._1} , ${j._2}")
          val uuid = assignUuid().toString
          values.append("'" + uuid + "'")
          val usefuldata = dataToInsert(connection, j._2.as[JsObject])
          usefuldata._2.append("UUID")
          usefuldata._3.append("'" + uuid + "'")
          insertPostgres(connection, usefuldata._1, usefuldata._2, usefuldata._3)
        }
        else {
          values.append(s"'${j._2.toString().replace("\"","")}'")
        }
      }
    }

    (table_name, column_names, values)
  }


  def sendToPostgres(connection: Connection, data: Vector[JsObject]) = {
    //antes venía como parámetro jsonObject y de ahí sacaban esto
    //val data = (jsonObject \ "data").as[JsArray].value
    for (i <- data){
      println("Objetos de data " + i)
      val usefuldata = dataToInsert(connection, i)
      insertPostgres(connection, usefuldata._1, usefuldata._2, usefuldata._3)
    }
  }

}
