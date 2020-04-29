package mario.tfm

import java.io.{DataOutputStream, InputStreamReader}
import java.net.{HttpURLConnection, URL, URLConnection}
import java.nio.charset.StandardCharsets

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.ContentType.WithCharset
import akka.http.scaladsl.model.MediaType.WithFixedCharset
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{ContentType, HttpCharset, HttpCharsets, HttpEntity, HttpHeader, HttpMethods, HttpRequest, HttpResponse, MediaType, MediaTypes, RequestEntity, StatusCodes}
import akka.http.scaladsl.server.directives.RouteDirectives.complete
import akka.http.scaladsl.settings.{ParserSettings, ServerSettings}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import akka.util.ByteString
import mario.tfm.postgresql.DataTools.sendToPostgres
import org.apache.http.entity.mime.MultipartEntityBuilder
import org.apache.http.impl.client.HttpClientBuilder
import play.api.libs.json.{JsObject, Json}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object PruebaHttpRequest extends App {

    //HttpRequest(request._1,"http://orion:1026/ngsi-ld/v1/entities",request._3, request._4, request._5)

     implicit val system = ActorSystem()

      val data = "{\"id\": \"urn:ngsi-ld:Subscription:pruebasubscribecontext\",\"type\": \"Subscription\",\"entities\": [{\"type\": \"Vehicle\"}], \"@context\": \"http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld\", \"description\": \"Notify me of all product vehicles changes\", \"notification\": {\"format\": \"normalized\",\"endpoint\": {\"uri\": \"http://windows:8080/rc\",\"accept\": \"application/ld+json\"}}}"


      val http = Http(system)
      implicit val ec : ExecutionContext = system.dispatcher

    // define custom media type:
    val utf8 = HttpCharsets.`UTF-8`
    val `application/ld+json`: WithFixedCharset =
      MediaType.customWithFixedCharset("application", "ld+json", utf8)

    // add custom media type to parser settings:
    val parserSettings = ParserSettings(system).withCustomMediaTypes(`application/ld+json`)
    val serverSettings = ServerSettings(system).withParserSettings(parserSettings)
    val jsonld = ContentType(`application/ld+json`)
    val p : WithFixedCharset = MediaType.applicationWithFixedCharset("ld+json",HttpCharsets.`UTF-8`,"json")
    val jsonld2 = ContentType(p)


    val j = MediaType.customWithFixedCharset(mainType= "application", subType= "ld+json", charset = HttpCharsets.`UTF-8`, fileExtensions = List("json"),
    params = Map.empty,
    allowArbitrarySubtypes = false)

    //val jsonld = ContentType(WithCharset(m., HttpCharsets),  HttpCharsets.`UTF-8`)
    //val jsonLdContentType = ContentType(MediaTypes.`application/ld+json')

      def postApplication(d: String): Future[HttpResponse] = {
        var aux = ""
        Try(Json.parse(d)) match {
          case Failure(t) => t
            throw new Exception("JSON not valid")
          case Success(h) =>
            try {
              aux = Json.stringify(h.as[JsObject] - "@context")
            }
            catch {
              case _ => "No se ha podido acceder a los datos"
                complete(StatusCodes.BadRequest)
            }
            Marshal(aux).to[RequestEntity] flatMap { ent =>
              println(ent)
              //http.content_type == "application/json"
              val request = HttpRequest(method = HttpMethods.POST, uri = "http://orion:1026/ngsi-ld/v1/subscriptions/", entity = ent.withContentType(MediaTypes.`application/json`)).withHeaders(RawHeader("Link", "<http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\""))
              println("REQUEST " + request)
              //ContentTypes.`application/json`  "http://orion:1026/ngsi-ld/v1/entities"
              http.singleRequest(request)
            }
        }
      }

        postApplication(data) onComplete {
          case Failure(ex) => System.out.println(s"Failed to post $data, reason: $ex")
          case Success(response) => System.out.println(s"Server responded with $response")
        }




  /*val result = Http("http://localhost:3000").postData(data)
    .header("Content-Type", "application/ld+json")
    .header("Accept", "application/ld+json")
    .option(HttpOptions.readTimeout(10000)).asString
}*/



  }



