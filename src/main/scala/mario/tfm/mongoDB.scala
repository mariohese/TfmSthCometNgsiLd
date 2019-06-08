package mario.tfm

import com.mongodb.ConnectionString
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.ServerAddress
import com.mongodb.client.MongoDatabase
import com.mongodb.client.MongoCollection
import org.bson.Document
import com.mongodb.async.SingleResultCallback
import java.util

import com.mongodb
import com.mongodb.Block
import com.mongodb.client.MongoCursor
import com.mongodb.client.model.Filters._
import com.mongodb.client.result.DeleteResult
import com.mongodb.client.model.Updates._
import com.mongodb.client.result.UpdateResult
import collection.JavaConverters._

object mongoDB extends App {

  import com.mongodb.Block

  val printDocumentBlock = new Block[Document]() {
    override def apply(document: Document): Unit = {
      println(document.toJson)
      println(document.toJson().getClass.getSimpleName)
    }
  }

  val callbackPrintDocuments = new SingleResultCallback[Document]() {
    override def onResult(document: Document, t: Throwable): Unit = {
      println(document.toJson)
    }
  }

  val callbackWhenFinished = new SingleResultCallback[Void]() {
    override def onResult(result: Void, t: Throwable): Unit = {
      println("Operation Finished!")
    }
  }

  // To directly connect to a single MongoDB server// To directly connect to a single MongoDB server

  // (this will not auto-discover the primary even if it's a member of a replica set)

  // "mongodb://192.168.249.128:27017"
  val mongoClientURI: MongoClientURI = new MongoClientURI("mongodb://kafka.localdomain:27017")
  val mongoClient: MongoClient = new MongoClient(mongoClientURI)
  val database = mongoClient.getDatabase("orion")
  val entities = database.getCollection("entities")

  val data = entities.find().forEach(printDocumentBlock)

}
