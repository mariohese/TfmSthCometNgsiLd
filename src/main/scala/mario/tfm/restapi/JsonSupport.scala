package mario.tfm.restapi

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import mario.tfm.restapi.UserRegistryActor.ActionPerformed
import spray.json.{DefaultJsonProtocol, JsArray, JsObject, JsString, JsValue, RootJsonFormat}
import java.util.logging.Logger

trait JsonSupport extends SprayJsonSupport {
  // import the default encoders for primitive types (Int, String, Lists etc)
  import DefaultJsonProtocol._

  val logger = Logger.getLogger(this.getClass.getSimpleName)

  implicit val userJsonFormat = jsonFormat3(User)
  implicit val usersJsonFormat = jsonFormat1(Users)
  //implicit val contextJsonFormat = jsonFormat2(NotificationData)

  implicit val actionPerformedJsonFormat = jsonFormat1(ActionPerformed)

 /* implicit object NotificationDataFormat extends RootJsonFormat[NotificationData] {
    def read(value: JsValue) = {
      logger.info(s"Llega el valor: ${value.toString()}")
      value.asJsObject.getFields("@context", "notifiedAt", "data") match {
        case Seq(JsString(context), JsString(notifiedAt), JsArray(data)) =>
          logger.info(s"context es: $context")
          logger.info(s"notifiedAt es: $notifiedAt")
          logger.info(s"data es: $data")
          //logger.info(s"data convertido es: ${data.map(_.toString())}")
          new NotificationData(context, notifiedAt, data.map(_.toString()))
      }
    }
    def write(obj: NotificationData) = JsObject()
  }*/



}
