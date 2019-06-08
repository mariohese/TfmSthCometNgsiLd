lazy val akkaHttpVersion = "10.1.8"
lazy val akkaVersion    = "2.6.0-M1"

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization    := "mario.tfm",
      scalaVersion    := "2.12.7"
    )),
    name := "tfmapi",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-http"            % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-http-xml"        % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-stream"          % akkaVersion,
      "com.typesafe.akka" %% "akka-slf4j" % "2.6.0-M1",
      "org.mongodb" % "mongodb-driver-async" % "3.10.2",
      "org.mongodb.scala" % "mongo-scala-driver_2.12" % "2.5.0",
      "org.mongodb" % "mongo-java-driver" % "3.6.4",
      "org.apache.commons" % "commons-rdf-api" % "0.5.0",
      "org.apache.commons" % "commons-rdf-jsonld-java" % "0.5.0",
      "org.apache.jena" % "jena-core" % "3.9.0",
      "org.apache.jena" % "jena-arq" % "2.10.0",
      "com.github.jsonld-java" % "jsonld-java" % "0.12.3",
      "org.scala-lang" % "scala-library" % "2.12.8",
      "com.typesafe.play" % "play-json_2.12" % "2.7.2",
      "org.postgresql" % "postgresql" % "42.1.1",
      "org.apache.flink" % "flink-scala_2.12" % "1.7.2",
      "org.apache.flink" % "flink-streaming-scala_2.12" % "1.7.2",
      "org.apache.flink" % "flink-clients_2.12" % "1.7.2",
      "org.apache.flink" % "flink-table_2.12" % "1.7.2",
      "org.apache.flink" % "flink-json" % "1.7.2",
      "com.github.jsonld-java" % "jsonld-java" % "0.12.3",
      "org.slf4j" % "slf4j-api" % "1.7.26",
      "org.slf4j" % "slf4j-log4j12" % "1.7.26",
      "org.apache.logging.log4j" % "log4j-core" % "2.11.2",
      "org.json4s" % "json4s-core_2.12" % "3.6.5",
      "org.json4s" % "json4s-jackson_2.12" % "3.6.5",
      "org.apache.flink" % "flink-core" % "1.7.2",
      "com.github.scopt" %% "scopt" % "4.0.0-RC2",
      "org.json" % "json" % "20180813",
      "com.typesafe.akka" %% "akka-http-testkit"    % akkaHttpVersion % Test,
      "com.typesafe.akka" %% "akka-testkit"         % akkaVersion     % Test,
      "com.typesafe.akka" %% "akka-stream-testkit"  % akkaVersion     % Test,
      "org.scalatest"     %% "scalatest"            % "3.0.5"         % Test
)
    
    
    
  )
