package mario.tfm.restapi

import java.util

import akka.actor.{ActorRef, ActorSystem}
import akka.event.Logging

import scala.concurrent.duration._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model.{ContentType, ContentTypes, HttpCharsets, HttpEntity, HttpMethods, HttpRequest, HttpResponse, MediaType, MediaTypes, RequestEntity, StatusCodes}
import akka.http.scaladsl.server.{Route, StandardRoute}
import mario.tfm.restapi.RestApiTools.postApplication
import akka.http.scaladsl.server.directives.{DebuggingDirectives, LoggingMagnet}
import akka.http.scaladsl.server.directives.MethodDirectives.delete
import akka.http.scaladsl.server.directives.MethodDirectives.get
import akka.http.scaladsl.server.directives.MethodDirectives.post
import akka.http.scaladsl.server.directives.RouteDirectives.complete
import akka.http.scaladsl.server.directives.PathDirectives.path

import scala.concurrent.{ExecutionContext, Future}
import akka.pattern.ask
import akka.util.Timeout
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import mario.tfm.orion_context_broker.TakeJson.json
import play.api.libs.json.{JsArray, JsObject, JsString, JsValue, Json}
import java.util.UUID
import java.util.logging.Logger

import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.{Http, HttpExt}
import akka.http.scaladsl.marshalling.Marshal
import akka.stream.ActorMaterializer
import mario.tfm.parameters.{Arguments, Config}
import mario.tfm.hdfs.HdfsTools._

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.{Failure, Success, Try}
//import mario.tfm.postgresql.DataTools._
//import mario.tfm.postgresql.SqlTools._
import scalaj.http.HttpOptions
import mario.tfm.restapi.Routes._
import mario.tfm.restapi.Routes
import org.apache.spark.sql.SparkSession

import mario.tfm.mysql.SqlTools._

//#user-routes-class
trait RoutesOrchestator {
  //#user-routes-class

  /*var connection: Connection = null
  try {
    connection = createConnection()
  }
  catch {
    case e => e.printStackTrace()
  }*/

  // we leave these abstract, since they will be provided by the App
  println("Se está ejecutando")
  implicit def system: ActorSystem
  implicit def ec: ExecutionContext = system.dispatcher
  implicit def http: HttpExt = Http(system)
  implicit def config: Config


  lazy val logger = Logger.getLogger(this.getClass.getSimpleName)

  // other dependencies that UserRoutes use
  //def userRegistryActor: ActorRef

  // Required by the `ask` (?) method below
  implicit lazy val timeout = Timeout(5.seconds) // usually we'd obtain the timeout from the system's configuration

  //#all-routes
  //#users-get-post
  //#users-get-delete

  lazy val RoutesOrchestator: Route =
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

    if (config.isSql == "yes"){

      // CREATE SQL CONNECTION
      implicit val connection = createConnection()

      concat (
        // rc de receive context
        pathPrefix("notification") {
          receiveContextSql
        },

        path("subscribeContext") {
          subscribeContext
        },
        // http://host:port/path?key=authType&value=Basic345

        pathPrefix ("data"){

          concat (

            pathEnd {
              get {
                //parameters('key.as[String], 'value.as[String]){
                // (key, value) =>
                println("Ha entrado aquí")
                complete(s"Completado")
              }
            },

            //Esta la añado yo, no está en el STH COMET actual
            path("type" / Segment){
              (tipo) =>
                get {
                  //parameters('key.as[String], 'value.as[String]){
                  // (key, value) =>
                  //dropTypeTable(connection, tipo)
                  complete(s"El tipo es $tipo")
                }
            },

            path("type" / Segment / "id" / Segment){
              (tipo, id) =>
                get {
                  //parameters('key.as[String], 'value.as[String]){
                  // (key, value) =>
                  //dropEntityData(connection, tipo, id)
                  complete(s"El tipo es $tipo y el id $id")
                }
            },
            path("type" / Segment / "id" / Segment / "attributes" / Segment){
              (tipo, id, attrName) =>
                concat (
                  pathEnd {
                    get {
                      parameters('aggrMethod?, 'aggrPeriod?,'dateFrom?, 'dateTo?,
                        'lastN?, 'hLimit?, 'hOffset?, 'fileType?, 'count.?){
                        (aggrMethod: Option[String], aggrPeriod: Option[String],
                         dateFrom: Option[String], dateTo: Option[String], lastN: Option[String],
                         hLimit: Option[String], hOffset: Option[String],
                         fileType: Option[String], count: Option[String]) =>

                          if ((aggrMethod isDefined) && (aggrPeriod isDefined)){
                            aggregatedInfo()
                            complete("Ha entrado en aggregated")
                          }
                          else if (lastN isDefined){
                            historicalInfo()
                            complete("Ha entrado en historical")
                          }
                          else if ((lastN isEmpty) && (hLimit isDefined) && (hOffset isDefined)){
                            historicalInfo()
                            //Este no funciona bien, me ha funcionado sin haber lastN ni hLimit ni hOffset
                            complete("Ha entrado en historical sin lastN")
                          }
                          else {
                            //dropEntityAttribute(connection, tipo, id, attrName)
                            complete("Ha entrado en dropEntityAttribute")
                          }
                      }
                    }
                  }
                )
            }
          )
        }
      )
    }

    else{

      import org.apache.log4j.Logger
      import org.apache.log4j.Level

      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)

      implicit val spark = SparkSession.builder
        .appName("SparkSession")
        .master("local[4]")
        .config("spark.network.timeout", 100000000)
        .config("spark.executor.heartbeatInterval",100000000)
        .config("spark.io.compression.codec", "snappy")
        .config("spark.rdd.compress", "true")
        .config("spark.streaming.backpressure.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.parquet.mergeSchema", "true")
        .config("spark.sql.parquet.binaryAsString", "true")
        .config("spark.hive.mapred.supports.subdirectories","true")
        .config("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive","true")
        .getOrCreate


      concat (
        // rc de receive context
        pathPrefix("notification") {
          receiveContextNoSql
        },

        path("subscribeContext") {
          subscribeContext
        },
        // http://host:port/path?key=authType&value=Basic345

        pathPrefix ("data"){

          concat (

            path("count" /"id" / Segment){
              (id) =>
                get {
                  parameters('dateFrom?, 'dateTo?){
                    (dateFrom: Option[String], dateTo: Option[String]) =>

                      val result = countGetId(id, dateFrom, dateTo)
                      complete(HttpEntity(ContentTypes.`application/json`, result))
                  }
                }
            },
            path("count" /"type" / Segment){
              (tipo) =>
                get {
                  parameters('dateFrom?, 'dateTo?){
                    (dateFrom: Option[String], dateTo: Option[String]) =>

                      val result = countGetType(tipo, dateFrom, dateTo)
                      complete(HttpEntity(ContentTypes.`application/json`, result))
                  }
                }
            },

            path("id" / Segment){
              (id) =>
                get {
                  parameters('dateFrom?, 'dateTo?, 'lastN?){
                    (dateFrom: Option[String], dateTo: Option[String],
                     lastN: Option[String]) =>

                      if (lastN isEmpty) {
                        val result = getId(id, dateFrom, dateTo)
                        complete(HttpEntity(ContentTypes.`application/json`, result))
                      }
                      else {
                        val result = getIdLastN(id, dateFrom, dateTo, lastN)
                        complete(HttpEntity(ContentTypes.`application/json`, result))
                      }
                  }
                }
            },
            path("type" / Segment){
              (tipo) =>
                get {
                  parameters('dateFrom?, 'dateTo?, 'lastN?){
                    (dateFrom: Option[String], dateTo: Option[String],
                     lastN: Option[String]) =>

                      if (lastN isEmpty) {
                        val result = getType(tipo, dateFrom, dateTo)
                        complete(HttpEntity(ContentTypes.`application/json`, result))
                      }
                      else {
                        val result = getTypeLastN(tipo, dateFrom, dateTo, lastN)
                        complete(HttpEntity(ContentTypes.`application/json`, result))
                      }
                  }
                }
            },

            path("id" / Segment / "property" / Segment){
              (id, property) =>
                concat (
                  pathEnd {
                    get {
                      parameters('aggrMethod?, 'aggrPeriod?,'dateFrom?, 'dateTo?){
                        (aggrMethod: Option[String], aggrPeriod: Option[String],
                         dateFrom: Option[String], dateTo: Option[String]
                         ) =>

                          val result = getAggrMethodPropertyId(id, property,
                            aggrMethod, aggrPeriod, dateFrom, dateTo)
                          complete(HttpEntity(ContentTypes.`application/json`, result))
                      }
                    }
                  }

                )
            },
            path("id" / Segment / "property" / Segment / Segment){
              (id, property, property2) =>
                concat (
                  pathEnd {
                    get {
                      parameters('aggrMethod?, 'aggrPeriod?,'dateFrom?, 'dateTo?){
                        (aggrMethod: Option[String], aggrPeriod: Option[String],
                         dateFrom: Option[String], dateTo: Option[String]
                        ) =>

                          val result = getAggrMethodPropertyId(id,
                            property + "." + property2,
                            aggrMethod, aggrPeriod, dateFrom, dateTo)
                          complete(HttpEntity(ContentTypes.`application/json`, result))
                      }
                    }
                  }
                )
            },
            path("type" / Segment / "property" / Segment){
              (tipo, property) =>
                concat (
                  pathEnd {
                    get {
                      parameters('aggrMethod?, 'aggrPeriod?,'dateFrom?, 'dateTo?){
                        (aggrMethod: Option[String], aggrPeriod: Option[String],
                         dateFrom: Option[String], dateTo: Option[String]
                        ) =>
                          val result = getAggrMethodPropertyType(tipo, property,
                            aggrMethod, aggrPeriod, dateFrom, dateTo)
                          complete(HttpEntity(ContentTypes.`application/json`, result))
                      }
                    }
                  }
                )
            },
            path("type" / Segment / "property" / Segment / Segment){
              (tipo, property, property2) =>
                concat (
                  pathEnd {
                    get {
                      parameters('aggrMethod?, 'aggrPeriod?,'dateFrom?, 'dateTo?){
                        (aggrMethod: Option[String], aggrPeriod: Option[String],
                         dateFrom: Option[String], dateTo: Option[String]
                        ) =>
                          val result = getAggrMethodPropertyType(tipo,
                            property + "." + property2,
                            aggrMethod, aggrPeriod, dateFrom, dateTo)
                          complete(HttpEntity(ContentTypes.`application/json`, result))
                      }
                    }
                  }
                )
            }
          )

        }
      )
    }


  //#all-routes
}
