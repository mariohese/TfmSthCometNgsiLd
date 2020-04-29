package mario.tfm.postgresql

import java.sql.{Connection, DriverManager}
import java.util.Properties
import java.util.logging.Logger

import scala.collection.mutable.ListBuffer

object SqlTools {

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
  def insertPostgres(connection: Connection, table_name: String, column_list: ListBuffer[String], values_list: ListBuffer[String]) = {

    var sql = ""
    if (table_name == "Property" || table_name == "Relationship"){
      sql = s"INSERT INTO ${table_name} (${column_list.mkString(",")}) VALUES (${values_list.mkString(",")})"
    }

    else {
      sql = s"INSERT INTO ${table_name} (time, ${column_list.mkString(",")}) VALUES (NOW(), ${values_list.mkString(",")})"
    }

    logger.info(s"El mensaje de INSERT es: ${sql}")
    println(s"El mensaje de INSERT es: ${sql}")
    //val statement = connection.prepareStatement(sql)

    //statement.setString(1, st)
    //statement.executeUpdate()
    //statement.close()
  }

  def dropTypeTable(connection: Connection, type_table: String) = {
    var sql = s"TRUNCATE TABLE ${type_table}"
    println(sql)
  }

  def dropEntityData(connection: Connection, type_table: String, id: String ) = {
    var sql = s"DELETE FROM ${type_table} WHERE id='${id}'"
    println(sql)
  }

  def dropEntityAttribute(connection: Connection, type_table: String, id: String, attribute: String) = {
    var sql = s"UPDATE ${type_table} SET ${attribute}=null WHERE id='${id}'"
    println(sql)
  }

  def aggregatedInfo() = {
    println("Ha entrado en aggregated")
  }

  def historicalInfo() = {
    println("Ha entrado en historical")
  }




}
