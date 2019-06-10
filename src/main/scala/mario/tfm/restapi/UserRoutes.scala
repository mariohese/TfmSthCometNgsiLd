package mario.tfm.restapi

import java.util

import akka.actor.{ActorRef, ActorSystem}
import akka.event.Logging

import scala.concurrent.duration._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model.{HttpRequest, StatusCodes}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.{DebuggingDirectives, LoggingMagnet}
import akka.http.scaladsl.server.directives.MethodDirectives.delete
import akka.http.scaladsl.server.directives.MethodDirectives.get
import akka.http.scaladsl.server.directives.MethodDirectives.post
import akka.http.scaladsl.server.directives.RouteDirectives.complete
import akka.http.scaladsl.server.directives.PathDirectives.path

import scala.concurrent.Future
import mario.tfm.restapi.UserRegistryActor._
import akka.pattern.ask
import akka.util.Timeout
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import mario.tfm.orion_context_broker.TakeJson.json
import play.api.libs.json.{JsArray, JsObject, JsString, JsValue, Json}
import java.util.UUID
import java.util.logging.Logger

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import mario.tfm.postgresql.DataInteraction.insertPostgres


//#user-routes-class
trait UserRoutes extends JsonSupport {
  //#user-routes-class


  // we leave these abstract, since they will be provided by the App
  implicit def system: ActorSystem

  lazy val log = Logging(system, classOf[UserRoutes])

  // other dependencies that UserRoutes use
  def userRegistryActor: ActorRef

  // Required by the `ask` (?) method below
  implicit lazy val timeout = Timeout(5.seconds) // usually we'd obtain the timeout from the system's configuration

  //#all-routes
  //#users-get-post
  //#users-get-delete

  lazy val userRoutes: Route =
  /*extractRequest { request =>
   println(s"Request uri: ${request.uri}")
   println(s"Request protocol: ${request.protocol}")
   println(s"Request entity: ${request.entity}")
   println(s"Request entity data bytes: ${request.entity.toString}")
   println(s"Request headers: ${request.headers}")
   println(s"Request method value: ${request.method.value}")
   println(s"Request method is ${request.method.name} and content-type is ${request.entity.contentType}")
   complete(s"Request method is ${request.method.name} and content-type is ${request.entity.contentType}")
 }*/


    pathPrefix("rc") {
      pathEnd {
        post {
          logger.info(s"Entity String: ${entity(as[String]).toString}")
          entity(as[String]) { jsonString =>
            val jsonObject: JsValue = Json.parse(jsonString)

            println("Ha entrado")

            //takeData(jsonObject)
            prepareSend(jsonObject)


            def assignUuid (): UUID = {
              val uuid = UUID.randomUUID()
              uuid
            }

            def dataToInsert(jsonobject: JsObject): (String, ListBuffer[String], ListBuffer[String]) = {
              var table_name: String = ""
              var column_names =  ListBuffer[String]()
              var values = ListBuffer[String]()
              logger.info(s"Json Object fields: ${jsonobject.fields}")
              for (j<- jsonobject.fields){
                logger.info(s"Mensaje: ${j.toString()}")
                if (j._1 == "type"){
                  logger.info(s"Type: ${j._1} , table_name ${j._2}")
                  table_name = j._2.toString().replace("\"","")
                }
                else {
                  column_names.append(j._1)
                  if (j._2.isInstanceOf[JsObject]){
                    logger.info(s"Esto es instancia de objeto: ${j._1} , ${j._2}")
                    val uuid = assignUuid().toString
                    values.append("'" + uuid + "'")
                    val usefuldata = dataToInsert(j._2.as[JsObject])
                    usefuldata._2.append("UUID")
                    usefuldata._3.append("'" + uuid + "'")
                    insertPostgres(usefuldata._1, usefuldata._2, usefuldata._3)
                  }
                  else {
                    values.append(s"'${j._2.toString().replace("\"","")}'")
                  }
                }
              }
              (table_name, column_names, values)
            }


            def prepareSend(d: JsValue) = {
              val data = (jsonObject \ "data").as[JsArray].value
              for (i <- data){
                val usefuldata = dataToInsert(i.as[JsObject])
                insertPostgres(usefuldata._1, usefuldata._2, usefuldata._3)
              }
            }

            /*def takeData(d: JsValue): Unit ={
              val data = (jsonObject \ "data").as[JsArray].value
              for (i <- data){
                val fields = i.as[JsObject].fields

                for (j <- fields) {
                  var table_name: String = ""
                  println(j)
                  if (j._1 == "type"){
                    table_name = j._2.toString()
                  }
                  println("Clave : " + j._1 + " " + j._1.getClass.getSimpleName)
                  if (j._2.isInstanceOf[JsObject]){
                    val uuid = assignUuid()
                  }
                  println("Valor : " + j._2 + " " + j._2.getClass.getSimpleName)

                }

              }
              // Hacer una función que recorra cada elemento del ArrayBuffer y que cuando lo que sea sea de tipo JsString
              // o String se envía a TimescaleDB y que si es de tipo JsObject se recorra el nuevo JsObject hasta enviar
              // a tablas todos sus elementos
            }*/



           /*

           val mapper: ObjectMapper = new ObjectMapper()
           var mapp = new util.HashMap[String, Object]()
            // convert JSON string to Map
           def parseJson(s: String) = {
            mapp = mapper.readValue((jsonObject \ "data").as[JsArray].toString(), new TypeReference[util.Map[String, Object]]() {})
            mapp
           }

            println(parseJson(jsonString))

            System.out.println("input: " + jsonString)
            System.out.println("output:")

            import scala.collection.JavaConverters._

            for (entry <- asScalaSet(parseJson(jsonString).entrySet)) {
              System.out.println("key: " + entry.getKey)
              System.out.println("value type: " + entry.getValue.getClass)
              System.out.println("value: " + entry.getValue.toString)

            }*/


          complete(StatusCodes.Created)

          }
        }
      }
    }

    pathPrefix("users") {
      concat(
        //#users-get-delete
        pathEnd {
          concat(
            get {
              val users: Future[Users] =
                (userRegistryActor ? GetUsers).mapTo[Users]
              println(users.toString)
              complete(users)
            },
            post {
              entity(as[User]) { user =>
                println("Nombre del usuario: " + user.name)
                val userCreated: Future[ActionPerformed] =
                  (userRegistryActor ? CreateUser(user)).mapTo[ActionPerformed]
                onSuccess(userCreated) { performed =>
                  log.info("Created user [{}]: {}", user.name, performed.description)
                  complete((StatusCodes.Created, performed))
                }
              }
            })
        },
        //#users-get-post
        //#users-get-delete
        path(Segment) { name =>
          concat(
            get {
              //#retrieve-user-info
              val maybeUser: Future[Option[User]] =
                (userRegistryActor ? GetUser(name)).mapTo[Option[User]]
              rejectEmptyResponse {
                complete(maybeUser)
              }
              //#retrieve-user-info
            },
            delete {
              //#users-delete-logic
              val userDeleted: Future[ActionPerformed] =
                (userRegistryActor ? DeleteUser(name)).mapTo[ActionPerformed]
              onSuccess(userDeleted) { performed =>
                log.info("Deleted user [{}]: {}", name, performed.description)
                complete((StatusCodes.OK, performed))
              }
              //#users-delete-logic
            })
        })
      //#users-get-delete
    }

  //#all-routes
}
