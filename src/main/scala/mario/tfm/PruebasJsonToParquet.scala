package mario.tfm

import org.apache.spark.sql.SparkSession

object PruebasJsonToParquet extends App {

  val data = "{\"id\": \"urn:ngsi-ld:OffStreetParking:Downtown02\",\"type\": \"OffStreetParking\",\"name\": {\"type\": \"Property\",\"value\": \"Downtown One\"},\"availableSpotNumber\": {\"type\": \"Property\",\"value\": 121,\"observedAt\": \"2018-12-04T12:00:00Z\",\"reliability\": {\"type\": \"Property\",\"value\": 0.7},\"providedBy\": {\"type\": \"Relationship\",\"object\": \"urn:ngsi-ld:Camera:C1\"}},\"totalSpotNumber\": {\"type\": \"Property\",\"value\": 200},\"location\": {\"type\": \"GeoProperty\",\"value\": {\"type\": \"Point\",\"coordinates\": [-8.5, 41.2]}},\"@context\": \"https://json-ld.org/contexts/person.jsonld\"}"
  val data2 = "{\"availableSpotNumber\": {\"observedAt\": \"2018-12-04T12:00:00Z\",\"providedBy\": {\"type\": \"Relationship\",\"object\": \"urn:ngsi-ld:Camera:C1\"}},\"@context\": \"https://json-ld.org/contexts/person.jsonld\"}"

  val spark = SparkSession
    .builder()
    .master("local")
    .appName("Spark SQL basic example")
    .config("HADOOP_CONF_DIR", "C:\\hadoop\\etc")
    .getOrCreate()

  import spark.implicits._
  val df = spark.read.json(Seq(data2).toDS())
  //val df = spark.read.parquet("hdfs://localhost:9000/json2.parquet")
  df.show()
  println(df.collect().size)
  println(df.show())

  df.printSchema
  df.write.parquet("hdfs://localhost:9000/json3.parquet")
}
