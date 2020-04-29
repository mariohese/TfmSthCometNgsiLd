package mario.tfm.mysql

import java.sql.{Connection, DriverManager}
import java.util.Properties
import java.util.logging.Logger

import mario.tfm.postgresql.SqlTools.logger

import scala.collection.mutable.ListBuffer

object SqlTools extends App {

  val logger = Logger.getLogger(this.getClass.getSimpleName)

  def createConnection(): Connection = {
    // Parametrizar esto en el Config
    // Formato: jdbc:postgresql://servidor/nombre_db
    var url = "jdbc:mysql://localhost:3306/tfm?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val props: Properties = new Properties()
    val driver = "com.mysql.cj.jdbc.Driver"

    // Parametrizar esto tb
    props.setProperty("user", "mario")
    props.setProperty("password", "1234")
    props.setProperty("ssl", "false")

    var connection: Connection = null
    try {
      Class.forName(driver)
      connection = DriverManager.getConnection(url, props)
    }
    catch {
      case e => e.printStackTrace()
    }

    val pre = connection.prepareStatement("SET sql_mode = ''")
    pre.execute()
    pre.close()

    connection
  }

  /*var sql = ""
  var connection = createConnection()

  sql = s"INSERT INTO entity (id, type, notifiedAt, context) VALUES (?, ?, CAST(? AS DATETIME),?)"

  val pre = connection.prepareStatement("SET sql_mode = ''")
  val statement = connection.prepareStatement(sql)
  pre.execute()
  pre.close()

  statement.setString(1, "Peugeot")
  statement.setString(2, "Vehiculo")
  statement.setString(3, "2018-12-04T12:00:00Z")
  statement.setString(4, "http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld")
  logger.info(s"El mensaje de INSERT es: ${sql}")
  statement.executeUpdate()
  statement.close()*/
  // Al final hay que hacer connection.close()

  // table_name se supone que tiene que ser el valor del parámetro type
  def insertMySql(table_name: String, column_list: ListBuffer[String], values_list: ListBuffer[String])
  (implicit connection: Connection) = {

    val logger = Logger.getLogger(this.getClass.getSimpleName)

    var sql = ""
    //if (table_name == "Property" || table_name == "Relationship"){
      sql = s"INSERT INTO ${table_name} (${column_list.mkString(",")}) VALUES (${values_list.mkString(",")})"
    //}

    /*else {
      sql = s"INSERT INTO ${table_name} (time, ${column_list.mkString(",")}) VALUES (NOW(), ${values_list.mkString(",")})"
    }*/


    val statement = connection.prepareStatement(sql)
    logger.info(sql)
    //statement.setString(1, st)


    statement.executeUpdate()
    statement.close()
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