package mario.tfm.orion_context_broker

import play.api.libs.json._

import scala.collection.mutable.ArrayBuffer

object StringToJsonList {

  def stringToJson(s: String): ArrayBuffer[JsObject] = {
    var array = ArrayBuffer.empty[JsObject]

    if (s.charAt(0) == '[') {
      array = Json.parse(s).as[ArrayBuffer[JsObject]]
    } else {
      array += Json.parse(s).as[JsObject]
    }
    array
  }

  def main(args: Array[String]): Unit = {

    var s = "[{\"id\":\"Room2\",\"type\":\"Room\",\"pressure\":{\"type\":\"Number\",\"value\":720,\"metadata\":{}},\"temperature\":{\"type\":\"Number\",\"value\":23,\"metadata\":{}}},{\"id\":\"Room1\",\"type\":\"Room\",\"pressure\":{\"type\":\"Integer\",\"value\":720,\"metadata\":{}},\"temperature\":{\"type\":\"Float\",\"value\":23,\"metadata\":{}}}]"
    var s2 = "{\"id\":\"Room2\",\"type\":\"Room\",\"pressure\":{\"type\":\"Number\",\"value\":720,\"metadata\":{}},\"temperature\":{\"type\":\"Number\",\"value\":23,\"metadata\":{}}}"
    var lista = stringToJson(s)
    var lista2 = stringToJson(s2)
    println(lista2)
    var elem0 = lista.apply(0)
    var picker = (__ \ 'temperature \ 'value).json.pickBranch
    var jssuccess = lista.apply(0).transform(picker)

    var value = jssuccess.get

    println("Valor")
    println(value.apply("temperature").apply("value"))
    println(value.apply("temperature").apply("value").getClass.getSimpleName)
    println(s"El tipo es ${value.apply("temperature")}")
    println(lista(0).apply("type"))

    println(lista(0).apply("type").getClass.getSimpleName)

    println(lista)
    println(lista.getClass.getSimpleName)

    println("Elemento 0")

    println(lista.apply(0))
    println(lista.apply(0).getClass.getSimpleName)
    println(lista.apply(0).apply("temperature").as[JsObject].keys)
    println("Elemento 1")
    println(lista.apply(1))
  }
}
