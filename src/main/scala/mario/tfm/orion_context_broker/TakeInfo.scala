package mario.tfm.orion_context_broker

import java.net.{HttpURLConnection, URL}

object TakeInfo extends App {

  def getOrionData() = {
    /*val urlString = new URL("http://orion:1026/ngsi-ld/v1/entities?type=OffStreetParking -H 'Accept: application/ld+json'")

    val connection = urlString.openConnection.asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(5000)
    connection.setReadTimeout(50000)
    connection.setRequestMethod("GET")
    val inputStream = connection.getInputStream
    val content = io.Source.fromInputStream(inputStream).mkString
    if (inputStream != null) inputStream.close

    content
  }
  println(getOrionData())*/
  }
}