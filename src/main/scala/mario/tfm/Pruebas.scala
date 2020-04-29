package mario.tfm

import java.io.ByteArrayOutputStream
import java.sql.{Connection, DriverManager}
import java.util.{Date, Properties}

import org.httprpc.io.JSONEncoder
import org.httprpc.sql.ResultSetAdapter
import play.api.libs.json.{JsObject, JsValue, Json}

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
  def insertPostgres() = {

    var connection: Connection = null
    try {
      connection = createConnection()

      val statement = connection.createStatement()

      val resultset = new ResultSetAdapter(statement.executeQuery("SELECT * FROM Prueba"))

      val jsonEncoder: JSONEncoder = new JSONEncoder()

      val c = new ByteArrayOutputStream()
      jsonEncoder.write(resultset, c)

      println(c.toString)


      statement.close()

    } catch {
      case e => e.printStackTrace()
    }

    connection.close()

  }



  val jsprueba = """{"id": "urn:ngsi-ld:OffStreetParking:Downtown02","type": "OffStreetParking","name": {"type": "Property","value": "Downtown One"},"availableSpotNumber": {"type": "Property","value": 121,"observedAt": "2018-12-04T12:00:00Z","reliability": {"type": "Property","value": 0.7},"providedBy": {"type": "Relationship","object": "urn:ngsi-ld:Camera:C1"}},"totalSpotNumber": {"type": "Property","value": 200},"location": {"type": "GeoProperty","value": {"type": "Point","coordinates": [-8.5, 41.2]}},"@context": "https://json-ld.org/contexts/person.jsonld"}"""

  val jsobject = Json.parse(jsprueba)
  val location = (jsobject \ "availableSpotNumber").as[JsObject]

  val l = location.toString()
  println(l)
  val value = (location \ "observedAt").as[String]
  println("'"+ value + "'")

  import java.text.DateFormat
  import java.text.SimpleDateFormat
  import java.util.TimeZone

  import java.time.LocalDateTime
  import java.time.format.DateTimeFormatter._

  val dts = "2018-12-04T12:00:10Z"
  val d = LocalDateTime.parse(dts, ISO_DATE_TIME)
  println(d.getClass.getSimpleName)

  //insertPostgres()


}
