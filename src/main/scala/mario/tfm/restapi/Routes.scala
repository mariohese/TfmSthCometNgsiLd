package mario.tfm.restapi

import java.sql.Connection
import java.util.logging.Logger

import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.{as, entity, extractRequest, pathEnd}
import akka.http.scaladsl.server.directives.PathDirectives.path
import akka.http.scaladsl.server.directives.RouteDirectives.complete
import mario.tfm.restapi.RestApiTools.postApplication
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.MethodDirectives.post
import mario.tfm.parameters.Config
import mario.tfm.hdfs.HdfsTools._

//import mario.tfm.postgresql.DataTools.sendToPostgres
import org.apache.spark.sql.SparkSession
import play.api.libs.json.{JsObject, JsValue, Json}

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}
import mario.tfm.mysql.DataToolsSql._


object Routes {

  lazy val logger = Logger.getLogger(this.getClass.getSimpleName)

  def subscribeContext (implicit ec: ExecutionContext, http: HttpExt) : Route = {
    pathEnd {
      extractRequest {
        request =>
          println("Request " + request)
          entity(as[String]) {
            data =>
              println(data)

              postApplication(ec, http, data) onComplete {
                case Failure(ex) => System.out.println(s"Failed to post $data, reason: $ex")
                case Success(response) => System.out.println(s"Server responded with $response")
              }

              complete(StatusCodes.Created)
          }
      }
    }
  }

  def receiveContextSql (implicit ec: ExecutionContext, http: HttpExt, connection: Connection) : Route = {
    pathEnd {
      post {
        logger.info(s"Entity String: ${entity(as[String]).toString}")
        entity(as[String]) { jsonString =>

          val jsonObject: JsValue = Json.parse(jsonString)

          Try(Json.parse(jsonString)) match {
            case Failure(t) => t
              complete(StatusCodes.BadRequest)
            case Success(h) =>
              try {
                val data = (h \ "data").as[Vector[JsObject]]
                println("data 1 " + data)
                // sendToPostgres(null, data)
                sendToMySql(data)
                complete(StatusCodes.Created)
              }
              catch {
                case _ => "No se ha podido acceder a los datos"
                  complete(StatusCodes.BadRequest)
              }
          }
          //takeData(jsonObject)
          //sendToPostgres(connection, jsonObject)

        }
      }
    }
  }

  def receiveContextNoSql (implicit ec: ExecutionContext, http: HttpExt, spark: SparkSession): Route = {

      // Esto modificarlo para el caso NoSQL
      pathEnd {
        post {
          logger.info(s"Entity String: ${entity(as[String]).toString}")
          entity(as[String]) { jsonString =>
            println("JSON STRING: " + jsonString)
            // val jsonObject: JsValue = Json.parse(jsonString)

            println("Ha entrado")
            Try(Json.parse(jsonString)) match {
              case Failure(t) => t
                complete(StatusCodes.BadRequest)
              case Success(h) =>
                try {
                  val data = h.as[JsObject]
                  println("data 1 " + data)
                  println("Antes de enviar a parquet")
                  sendParquet(data)
                  complete(StatusCodes.Created)
                }
                catch {
                  case _ => "No se ha podido acceder a los datos"
                    complete(StatusCodes.BadRequest)
                }
            }

            //takeData(jsonObject)
            //sendToPostgres(connection, jsonObject)

          }
        }
      }



  }

  //En los siguientes métodos meter el Config como implícito.


}
