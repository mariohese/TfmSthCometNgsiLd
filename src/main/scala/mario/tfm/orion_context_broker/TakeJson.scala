package mario.tfm.orion_context_broker

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import org.json4s.DefaultFormats


object TakeJson extends App {

  var s = "[{\"id\":\"Room2\",\"type\":\"Room\",\"pressure\":{\"type\":\"Number\",\"value\":720,\"metadata\":{}},\"temperature\":{\"type\":\"Number\",\"value\":23,\"metadata\":{}}},{\"id\":\"Room1\",\"type\":\"Room\",\"pressure\":{\"type\":\"Integer\",\"value\":720,\"metadata\":{}},\"temperature\":{\"type\":\"Float\",\"value\":23,\"metadata\":{}}}]"
  var s2 = "{\"id\":\"Room3\",\"type\":\"Room\",\"pressure\":{\"type\":\"Number\",\"value\":720,\"metadata\":{}},\"temperature\":{\"type\":\"Number\",\"value\":23,\"metadata\":{}}}"

  var json = "{\"id\": \"urn:ngsi-ld:OffStreetParking:Downtown02\",\"type\": \"OffStreetParking\",\"name\": {\"type\": \"Property\",\"value\": \"Downtown One\"},\"availableSpotNumber\": {\"type\": \"Property\",\"value\": 121,\"observedAt\": \"2017-07-29T12:05:02\",\"reliability\": {\"type\": \"Property\",\"value\": 0.7},\"providedBy\": {\"type\": \"Relationship\",\"object\": \"urn:ngsi-ld:Camera:C1\"}},\"totalSpotNumber\": {\"type\": \"Property\",\"value\": 200},\"location\": {\"type\": \"GeoProperty\",\"value\": {\"type\": \"Point\",\"coordinates\": [-8.5, 41.2]}},\"@context\": \"https://json-ld.org/contexts/person.jsonld\"}"
  /*implicit val formats = DefaultFormats
  var map = parse(s).extract[Seq[Map[String, Any]]]
  var map2 = parse(s2).extract[Map[String, Any]]
  var jsonld = parse(json).extract[Map[String, Any]]

  println(map(0))

  println(map2)
  println(jsonld)
  */
  /*import java.util

  val mapper: ObjectMapper = new ObjectMapper()
  var mapp = new util.HashMap[String, Object]()
  // convert JSON string to Map
  mapp = mapper.readValue(json, new TypeReference[util.Map[String, Object]]() {})

  System.out.println("input: " + json)
  System.out.println("output:")

  import scala.collection.JavaConverters._

  for (entry <- asScalaSet(mapp.entrySet)) {
    System.out.println("key: " + entry.getKey)
    System.out.println("value type: " + entry.getValue.getClass)
    System.out.println("value: " + entry.getValue.toString)
  }*/


}
