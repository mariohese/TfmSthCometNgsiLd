package mario.tfm.restapi

//#context-receiver-actor
import java.util

import akka.actor.{Actor, ActorLogging, Props}
import akka.util.ByteString
import spray.json.{JsObject, JsString}

import scala.collection.immutable.Vector


final case class NotificationData(`@context`: String, notifiedAt: String, data: String)

object ContextReceiverActor {

}

class ContextReceiverActor {
  import ContextReceiverActor._
}