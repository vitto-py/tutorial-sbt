package content

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.common.serialization.StringSerializer
import java.time.{LocalDateTime, ZoneOffset}

object bankBalance {
    def main(args: Array[String]): Unit = {
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
            producer.send(prodRecord)
        }

        producer.close()
    }


    def newRandomRecord(name: String): ProducerRecord[String, String] = {
        val bparams = Map(
            "name"-> name,
            "amount"-> 1.3,
            "timestamp" -> LocalDateTime.now(ZoneOffset.UTC)
        )
        new ProducerRecord[String,String]("banking-records", name, bparams.toString())
    }

}
