package content

// the case class (make it a resource)
import content.bankTransactionProducer.BankRecord
import upickle.default._

import java.util.Properties
import java.util.concurrent.TimeUnit
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

// SERDE
import io.circe.{Decoder, Encoder}
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes._

//implicit conversion
import org.apache.kafka.streams.scala.ImplicitConversions._

/*
Start Kafka
zookeeper-server-start ~/confluent-7.5.3/etc/kafka/zookeeper.properties
kafka-server-start ~/confluent-7.5.3/etc/kafka/server.properties

See your topics
kafka-topics --list --bootstrap-server localhost:9092

Read your topics
kafka-console-consumer --topic banking-records --bootstrap-server localhost:9092 --from-beginning
*/


object bankStreamBalance extends App {

    val props: Properties = {
        val p = new Properties()
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-application")
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        //exactly once
        p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)
        p
    }
    // ---------------------------- SINGLETONS -----------------------------------
    object Topic {
        // alt shift = multiple cursor
        val topicBankRecords = "banking-records"
    }
    import Topic._
    // ---------------------------- SERDES BankRecord ----------------------------
    implicit def serdeRecord: Serde[BankRecord] = {
        //orderRecord T must be a CASE CLASS -> to Json -> to String -> To Bytes
        val serializer = (orderRecord: BankRecord) => orderRecord.asJson.noSpaces.getBytes()
        //from Array[Bytes] -> To String -> To CASE CLASS T Option
        val deserializer = (aAsBytes: Array[Byte]) => {
            val string = new String(aAsBytes)
            decode[BankRecord](string).toOption //decodes comes in circe decode[type to cast to](from type) => result is an Either => you must cast it to toOption
        }
        Serdes.fromFn[BankRecord](serializer, deserializer)
    }
    // ----------------------------------------------------------------------

    // ------------------------------------------------- KAFKA STREAM -------------------------------------------------
    val builder = new StreamsBuilder()
    // get the records -> <key, value> = <"Vitto", {"user":"Vitto","balance":43.90826416015625,"timestamp":"2025-05-04T09:58:18.776565645"}>
    val bankingRecords: KStream[String, String] = builder.stream[String, String](topicBankRecords)
    // so basically you have to turn the <String> back into a JSON
    // ----------------------------------- the easy way -----------------------------------
    val aggRecords: KTable[String, Double] = bankingRecords
        .mapValues(value => ujson.read(value)("balance").num.toDouble) //ujson.Value can be anything and of any format
        .groupByKey // group by user
        .aggregate[Double](0.0){(key, value, aggregate) => value + aggregate}

    // ----------------------------------- the correct way - TYPE SAFETY -----------------------------------
    val aggRecordsWithTypeSafety: KStream[String, BankRecord] = bankingRecords.mapValues { jsonString =>
        decode[BankRecord](jsonString).getOrElse {
            throw new RuntimeException(s"Failed to decode: $jsonString")
        }
    }
    // plus some nice printing
    aggRecordsWithTypeSafety.foreach{
        (key, valueRecord) => println(s"Received record for user ${valueRecord.user} with balance ${valueRecord.balance}")
    }
    // ---------------------------------------------------------------------------------------------------------


    // --------- NEW TOPIC ----------
    // plus conversion from Double to String
    /*
    WHY stringSerde and not doubleSerde? because otherwise you will have to specify the deserializer tool
    for reading the topic
    kafka-console-consumer --topic aggregated-balances --bootstrap-server localhost:9092 \
    --from-beginning \
    --property print.key=true \
    --property print.value=true
    ---------------------- RESULT ----------------------
    Betty	@kemp
    Vitto	@n���
    Anonymous	@r� �
    Dani	@yc�
    --------------------------------------------------------
    HOW TO READ THIS TOPIC IF YOU SELECT TO CONTINUE with Double
    kafka-console-consumer \
      --topic aggregated-balances \
      --bootstrap-server localhost:9092 \
      --from-beginning \
      --property print.key=true \
      --property print.value=true \
      --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
      --property value.deserializer=org.apache.kafka.common.serialization.DoubleDeserializer

    */
    aggRecords
        .mapValues(_.toString) //convert to string to avoid deserializer specification
        .toStream
        .to("aggregated-balances")(Produced.`with`(Serdes.stringSerde, Serdes.stringSerde))
    //(Materialized.with(Serdes.stringSerde, Serdes.doubleSerde))


    val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
    streams.start()

    sys.ShutdownHookThread {
        streams.close(10, TimeUnit.SECONDS)
    }
}
