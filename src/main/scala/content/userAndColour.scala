package content

import io.circe.{Decoder, Encoder}
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.scala.kstream.{KGroupedStream, KStream, KTable}
import org.apache.kafka.streams.scala.ImplicitConversions._


object userAndColour {
    object typesLocal {
        type UserIdColor = String
        type UserId = String
        type Colour = String
    }

    object topicLocal {
        //<null,String> = <null,"john,red">
        val UserIdColour = "user-and-colour"
        val parsedUserIdColor = "parsed-userid-and-color"
        val topicWordCount = "word-count-table"
    }

    import typesLocal._
    import topicLocal._

    // ------------------------------------------------- DECODE ENCODE -------------------------------------------------
    // This part just take it as it is, it comes from Documentation, and you must always do it like this
    // The main idea is that you need an implicit function capable of serialize and deserialize an Array of Bytes
    implicit def serdeOrder[T >: Null : Decoder: Encoder]: Serde[T] = { //serde is a type based on Kafka
        val serializer = (order: T) => order.asJson.noSpaces.getBytes()
        val deserializer = (aAsBytes: Array[Byte]) => {
            val string = new String(aAsBytes) //your TYPE
            decode[T](string).toOption //decodes comes in circe decode[type to cast to](from type) => result is an Either => you must cast it to toOption
        }
        Serdes.fromFn[T](serializer, deserializer)
    }
    // ------------------------------------------------- KAFKA STREAM -------------------------------------------------
    // topology = defines how the data has to flow through the kafka stream
    val builder = new StreamsBuilder()

    // step 1 - read KStream
    val topictextLine: KStream[String, UserIdColor] = builder.stream[String, UserIdColor](UserIdColour)
    
    val parsedStream: KStream[UserId, Colour] = topictextLine
        .filter((key, value) => value.contains(","))
        .selectKey[UserId]((key,value) => value.split(',')(0).toLowerCase)
        .mapValues(_.split(',')(1).toLowerCase)
        .filterNot((key, value) => Set("BlueG", "AnotherValue").contains(value))
        //!filter(if element eval = FALSE)

    // send to intermediate object for later pick it up as a KTABLE
    parsedStream.to(parsedUserIdColor)

    //step 2 - KTABLE
    val ktUserColor: KTable[UserId, Colour] = builder.table[UserId,Colour](parsedUserIdColor)

    // step 3 . wordCount
    val ktWordCounting: KTable[UserId, Long] = ktUserColor
        .groupBy((userid: UserId,color: Colour) => (color, color))
        .count()

    // output this KTABLE
    ktWordCounting.to(topicWordCount)
}
