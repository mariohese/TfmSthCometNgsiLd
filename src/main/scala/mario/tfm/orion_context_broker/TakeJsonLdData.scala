package mario.tfm.orion_context_broker

import java.io.ByteArrayInputStream

import com.github.jsonldjava.core.{JsonLdOptions, JsonLdProcessor}
import com.github.jsonldjava.utils.JsonUtils

import scala.collection.mutable

object TakeJsonLdData extends App {

  val jsonld = "{\"id\": \"urn:ngsi-ld:OffStreetParking:Downtown1\",\"type\": \"OffStreetParking\",\"name\": {\"type\": \"Property\",\"value\": \"Downtown One\"},\"availableSpotNumber\": {\"type\": \"Property\",\"value\": 121,\"observedAt\": \"2017-07-29T12:05:02\",\"reliability\": {\"type\": \"Property\",\"value\": 0.7},\"providedBy\": {\"type\": \"Relationship\",\"object\": \"urn:ngsi-ld:Camera:C1\"}},\"totalSpotNumber\": {\"type\": \"Property\",\"value\": 200},\"location\": {\"type\": \"GeoProperty\",\"value\": {\"type\": \"Point\",\"coordinates\": [-8.5, 41.2]}},\"@context\": \"https://json-ld.org/contexts/person.jsonld\"}"

  val inputStream = new ByteArrayInputStream(jsonld.getBytes())
  // Read the file into an Object (The type of this object will be a List, Map, String, Boolean,// Read the file into an Object (The type of this object will be a List, Map, String, Boolean,

  // Number or null depending on the root object in the file).
  val jsonObject = JsonUtils.fromInputStream(inputStream)
  // Create a context JSON map containing prefixes and definitions
  val context = new mutable.HashMap()
  // Customise context...
  // Create an instance of JsonLdOptions with the standard JSON-LD options
  val options = new JsonLdOptions
  // Customise options...
  // Call whichever JSONLD function you want! (e.g. compact)
  val compact = JsonLdProcessor.compact(jsonObject, context, options)
  // Print out the result (or don't, it's your call!)
  System.out.println(JsonUtils.toPrettyString(compact))
}
