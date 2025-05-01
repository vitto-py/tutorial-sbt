package content

import org.slf4j.LoggerFactory
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.common.serialization.StringSerializer
import java.time.{LocalDateTime, ZoneOffset}
/*
Start Kafka
zookeeper-server-start ~/confluent-7.5.3/etc/kafka/zookeeper.properties
kafka-server-start ~/confluent-7.5.3/etc/kafka/server.properties

See your topics
kafka-topics --list --bootstrap-server localhost:9092

Read your topics
kafka-console-consumer --topic banking-records --bootstrap-server localhost:9092 --from-beginning
*/
object bankBalance {
    private val logger = LoggerFactory.getLogger("bankBalance")
    def main(args: Array[String]): Unit = {
        logger.info("Kafka producer starting...")
        //props
        val props: Properties = {
            val p = new Properties()
            p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
            p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
            p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
            p
        }

        val producer: KafkaProducer[String, String]  = new KafkaProducer[String, String](props)
        for (i <- Range(0,15)){
            val result = (i%2,i%3) match {
                case (0,0) => "Vitto"
                case (0,_) => "Dani"
                case (_,0) => "Betty"
                case _ => "Anonymous"
            }
            val prodRecord: ProducerRecord[String, String] = newRandomRecord(result)
            logger.debug(s"Sending record: $prodRecord")
            producer.send(prodRecord)
        }

        producer.close()
        logger.info("Kafka producer finished.")
    }


    def newRandomRecord(name: String): ProducerRecord[String, String] = {
        val rand = new scala.util.Random
        val bparams = Map(
            "name"-> name,
            "amount"-> rand.nextFloat()*100,
            "timestamp" -> LocalDateTime.now(ZoneOffset.UTC)
        )
        new ProducerRecord[String,String]("banking-records", name, bparams.toString())
    }

}
