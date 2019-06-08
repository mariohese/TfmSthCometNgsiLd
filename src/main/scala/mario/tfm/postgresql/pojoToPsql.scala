package mario.tfm.postgresql

import java.sql.{Connection, DriverManager}
import java.util.Properties

object pojoToPsql extends App{

  var u: Usuario = new Usuario("Mario", 23)

  var insert : String = "INSERT INTO " + u.getClass.getSimpleName

  def getStatement (s: String)={
    var columnNames: String = " ("
    var values: String = "VALUES ("
    classOf[Usuario]
      .getDeclaredFields
      .map{ f => f.setAccessible(true)
        columnNames += f.getName + ","
        if (f.get(u).getClass.getSimpleName == classOf[String]){
          values += "\"" + f.get(u) + "\"" + ","
        }
        else values += f.get(u) + ","
      }
    s + columnNames.substring(0,columnNames.length - 1) + ") " + values.substring(0, values.length-1) + ")"
  }


  var url = "jdbc:postgresql://kafka.localdomain:5433/postgres"
  val props: Properties = new Properties()

  props.setProperty("user", "postgres")
  props.setProperty("password", "psql")
  props.setProperty("ssl", "false")

  var connection: Connection = null

  try {
    connection = DriverManager.getConnection(url, props)
    /*
        // Para crear una tabla
        val table_name = "prueba"
        val prepare_statement = connection.prepareStatement(s" CREATE TABLE IF NOT EXISTS $table_name(info jsonb UNIQUE NOT NULL)")
        prepare_statement.executeUpdate()
        prepare_statement.close()

        var st = "{\"id\":\"Room2\",\"type\":\"Room\",\"pressure\":{\"type\":\"Number\",\"value\":720,\"metadata\":{}},\"temperature\":{\"type\":\"Number\",\"value\":23,\"metadata\":{}}}"

        var column_label_list_string = "info"*/

    // Insertar un json en postgresql
    //val sql = s"INSERT INTO ${table_name}(${column_label_list_string}) VALUES (?::jsonb)"

    var sql = getStatement(insert)
    println(getStatement(insert))
    println(sql)
    val statement = connection.prepareStatement(sql)

    //statement.setString(1, st)
    statement.executeUpdate()
    statement.close()

  } catch {
    case e => e.printStackTrace()
  }

  connection.close()


}
