ThisBuild / version := "0.1.0-SNAPSHOT"
// ThisBuild -> scopes to Project
ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .settings(
      name := "kafkaScala",
      libraryDependencies ++= Seq(
          "org.apache.kafka" % "kafka-clients" % "2.8.0",
          "org.apache.kafka" % "kafka-streams" % "2.8.0",
          "org.apache.kafka" %% "kafka-streams-scala" % "2.8.0",
          "io.circe" %% "circe-core" % "0.14.1",
          "io.circe" %% "circe-generic" % "0.14.1",
          "io.circe" %% "circe-parser" % "0.14.1",
          // https://mvnrepository.com/artifact/com.lihaoyi/upickle
          "com.lihaoyi" %% "upickle" % "4.1.0",
          // SLF4J API
          // https://mvnrepository.com/artifact/org.slf4j/slf4j-api
          "org.slf4j" % "slf4j-api" % "2.0.17",
          // Logback implementation
          "ch.qos.logback" % "logback-classic" % "1.5.18",
          // SIMPLE
          /*slf4j-simple is a different backend from logback, and it takes precedence if both are present.
          It ignores your logback.xml completely.*/
          // https://mvnrepository.com/artifact/org.slf4j/slf4j-simple
          //"org.slf4j" % "slf4j-simple" % "2.0.17"
      )
  )
