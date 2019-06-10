package mario.tfm

import java.sql.{Connection, DriverManager}

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import java.util
import java.util.Properties
import java.util.UUID

import mario.tfm.postgresql.DataInteraction.logger

import scala.collection.mutable.ListBuffer

object Pruebas extends App {


  def createConnection() = {
    // Formato: jdbc:postgresql://servidor/nombre_db
    var url = "jdbc:postgresql://192.168.99.102:5432/postgres"
    val props: Properties = new Properties()

    props.setProperty("user", "postgres")
    props.setProperty("password", "password")
    props.setProperty("ssl", "false")

    var connection: Connection = null
    try {
      connection = DriverManager.getConnection(url, props)
    }
    catch {
      case e => e.printStackTrace()
    }
    connection
  }

  // table_name se supone que tiene que ser el valor del parámetro type
  def insertPostgres(table_name: String, column_list: ListBuffer[String], values_list: ListBuffer[Object]) = {

    var connection: Connection = null
    try {
      connection = createConnection()

      // Para crear una tabla... Pero las tablas ahora las crearé en local y en el proyecto hago que comiencen creadas en un Dockerfile
      // val table_name = "prueba"
      /*val prepare_statement = connection.prepareStatement(s" CREATE TABLE IF NOT EXISTS prueba(info jsonb UNIQUE NOT NULL)")
      prepare_statement.executeUpdate()
      prepare_statement.close()*/

      //var st = "{\"id\":\"Room2\",\"type\":\"Room\",\"pressure\":{\"type\":\"Number\",\"value\":720,\"metadata\":{}},\"temperature\":{\"type\":\"Number\",\"value\":23,\"metadata\":{}}}"

      //var column_label_list_string = "info"

      // Insertar un json en postgresql
      val sql = s"INSERT INTO ${table_name} (${column_list.mkString(",")}) VALUES (${values_list.mkString(",")})"

      logger.info(s"El mensaje de INSERT es: ${sql}")
      val statement = connection.prepareStatement(sql)

      //statement.setString(1, st)
      statement.executeUpdate()
      statement.close()

    } catch {
      case e => e.printStackTrace()
    }

    connection.close()

  }

  val uuid = UUID.randomUUID()
  val columnas = ListBuffer("id", "brandName", "isParked")
  val valores: ListBuffer[Object] =  ListBuffer("'urn:ngsi-ld:Vehicle:A4568'", "'" + uuid + "'", "'" + uuid + "'")

  println(columnas.mkString(","))
  println(valores.mkString(","))

  insertPostgres("Vehicle", columnas, valores)

}
