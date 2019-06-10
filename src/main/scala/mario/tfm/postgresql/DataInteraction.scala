package mario.tfm.postgresql
import java.sql.{Connection, DriverManager}
import java.util.Properties
import java.util.logging.Logger

import org.postgresql._
import org.postgresql.util.PGobject

import scala.collection.mutable.ListBuffer

object DataInteraction {

  val logger = Logger.getLogger(this.getClass.getSimpleName)

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
  def insertPostgres(table_name: String, column_list: ListBuffer[String], values_list: ListBuffer[String]) = {

    var connection: Connection = null
    try {
      connection = createConnection()

      // Para crear una tabla... Pero las tablas ahora las crearé en local y en el proyecto hago que comiencen creadas en un Dockerfile
      // val table_name = "prueba"
      //val prepare_statement = connection.prepareStatement(s" CREATE TABLE IF NOT EXISTS $table_name(info jsonb UNIQUE NOT NULL)")
      //prepare_statement.executeUpdate()
      //prepare_statement.close()

      //var st = "{\"id\":\"Room2\",\"type\":\"Room\",\"pressure\":{\"type\":\"Number\",\"value\":720,\"metadata\":{}},\"temperature\":{\"type\":\"Number\",\"value\":23,\"metadata\":{}}}"

      //var column_label_list_string = "info"

      // Insertar un json en postgresql
      var sql = ""
      if (table_name == "Property" || table_name == "Relationship"){
        sql = s"INSERT INTO ${table_name} (${column_list.mkString(",")}) VALUES (${values_list.mkString(",")})"
      }
        
      else {
        sql = s"INSERT INTO ${table_name} (time, ${column_list.mkString(",")}) VALUES (NOW(), ${values_list.mkString(",")})"
      }

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

}
