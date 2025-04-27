package content

import io.circe.{Decoder, Encoder}
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{GlobalKTable, JoinWindows}
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.scala.kstream.{KGroupedStream, KStream, KTable}
import org.apache.kafka.streams.scala.ImplicitConversions._

import java.time.Duration
import java.time.temporal.ChronoUnit


object KafkaStreams {
    // Remessa Online
    object Domain {
        type UserId = String
        type Profile = String
        type Product = String
        type OrderId = String
        type Status = String

        // case classes are classes that can be serialized! (which you will need for kafka
        case class Order(orderId: OrderId, userId: UserId, products: List[Product], amount: Double)
        case class Discount(profile: Profile, amount: Double)
        case class Payment(orderId: OrderId, status: Status)
    }

    // kafka data
    object Topic {
        // alt shift = multiple cursor
        val OrderByUser = "orders-by-user"
        val DiscountProfilesByUserTopic = "discount-profiles-by-user"
        val DiscountsTopic = "discounts"
        val OrdersTopic = "orders"
        val PaymentsTopic = "payments"
        val PaidOrdersTopic = "paid-orders"
    }
    // source = emit messages (elements)
    // flow = transform elements along the wy
    // sink = where messages are consume

    // ------------------------------------------------- DECODE ENCODE -------------------------------------------------
    /* This part just take it as it is, it comes from Documentation, and you must always do it like this
    * The main idea is that you need an implicit function capable of serialize and deserialize an Array of Bytes
    */

    // circe can turn our classes into JSON for that you need this code

    // this is a better explanation before going to Generics -> using Order -------------------------------------------------
    /* import Domain._
    implicit val serdeOrder: Serde[Order] = { //serde is a type based on Kafka
        val serializer = (order: Order) => order.asJson.noSpaces.getBytes()
        val deserializer = (aAsBytes: Array[Byte]) => {
            val string = new String(aAsBytes)
            decode[Order](string).toOption //decodes comes in circe decode[type to cast to](from type) => result is an either => you must cast it to toOption
        }
        Serdes.fromFn[Order](serializer, deserializer)
    } -------------------------------------------------
    */
    // ------------------------------------------------- WITH GENERICS -------------------------------------------------
    // >: Null -> mayor que null (no puede ser null)
    import Domain._
    import Topic._
    implicit def serdeOrder[T >: Null : Decoder: Encoder]: Serde[T] = { //serde is a type based on Kafka
        val serializer = (order: T) => order.asJson.noSpaces.getBytes()
        val deserializer = (aAsBytes: Array[Byte]) => {
            val string = new String(aAsBytes)
            decode[T](string).toOption //decodes comes in circe decode[type to cast to](from type) => result is an Either => you must cast it to toOption
        }
        Serdes.fromFn[T](serializer, deserializer)
    }

    // ------------------------------------------------- KAFKA STREAM -------------------------------------------------
    // topology = defines how the data has to flow through the kafka stream
    val builder = new StreamsBuilder()

    // ------------------------------------------------- TYPES OF STREAM -------------------------------------------------
    // STREAM[keyType, valType] -------------------------------------------------
    // most fundamental component kStream[key, value] IMPORT THE Stream that has ONE TOPIC
    // error by implicits -> Stupid Java legacy thing -> you have to use the implicit serdeOrder -> import org.apache.kafka.streams.scala.ImplicitConversions._ will allow this
    val usersOrderstram: KStream[UserId, Order] = builder.stream[UserId, Order](OrderByUser) // you must know your topics key and value
    val paymentsStream: KStream[OrderId, Payment] = builder.stream[OrderId, Payment](PaymentsTopic) // you must know your topics key and value

    // ------------------------------------------------- KTable -------------------------------------------------
    // this is another basic unit of kafka, its similar to a KStream but it differs on what can be called a WINDOW
    // elements in a ktable have a life expectancy -> streams come and go, ktables live for n-time -> more similar to unbounded dataframes of Spark
    // implicitly this means that elements on a KTABLE are stored/kept on the Broker
    val userProfileTable: KTable[UserId, Profile] = builder.table[UserId, Profile](DiscountProfilesByUserTopic)
    // Kafka Topic Creation Console Command
    // kafka-topics --bootstrap-server localhost:9092 --topic discount-profiles-by-user --create --config "cleanup.policy=compact"

    // GlobalKTable -------------------------------------------------
    /* variant of a KTable - difference - ktables can be partitioned, while Global don't, also GlobalKTables come from topics that are COPIED to all KSERVERS (nodes)
    what is the advantage? Performance!  GlobalKTable are on the memory of every node, hence, joins to GlobalKTables are fast*/
    val discountProfileGTable: GlobalKTable[Profile, Discount] = builder.globalTable[Profile, Discount](DiscountsTopic)
    // Kafka Topic Creation Console Command
    // kafka-topics --bootstrap-server localhost:9092 --topic discounts --create --config "cleanup.policy=compact"


    // Transformations -------------------------------------------------
    // the result is also an Stream
    // FILTER
    val expensiveOrders = usersOrderstram.filter((userId, order) => order.amount > 1000)

    // ONLY Values
    val listOfProducts = usersOrderstram.mapValues( order => order.products)

    // flatMap
    val productStream = usersOrderstram.flatMapValues(_.products) //order => order.products

    // JOIN [Stream, Table] (inner join) => yields [Stream]
    val ordersWithUserProfiles = usersOrderstram.join(userProfileTable){ //the JOIN (inner join) is done over the EQUAL param -> UserId
        (order, profile) => (order, profile) //just return everything as it is
    }

    // JOIN with CUSTOM KEY :[Stream, GTable] = [Stream]
    // the key value is the key of the "left" stream
    val discoutedOrdersStream = ordersWithUserProfiles.join(discountProfileGTable)(
        // ordersWithUserProfiles: UserId, (Order, Profile) -> I only need to retrieve the Profile
        {case (userid, (order, profile)) => profile}, //key of the join - picked from the "left" stream
        // on the second level, you have the (values from "left", values from "right")
        // case classes come with the copy method by default
        {case ((order, profile), discount) => order.copy(amount = order.amount - discount.amount)} //what to do values of the matched records
        //this is the return, a Order
    )

    // PICK ANOTHER IDENtifier for a given stream
    val ordersStream = discoutedOrdersStream.selectKey((userId, order) => order.orderId) //discoutedOrdersStream[UserId, Order] =>  ordersStream[orderId, Order]

    // JOIN [Streams, Stream] -> requires WINDOW
    val joinWindow = JoinWindows.of(Duration.of(5, ChronoUnit.MINUTES))
    val functionForJoinValues = (order: Order, payment: Payment) => if (payment.status == "PAID") Option(order) else Option.empty[Order]
    //Stream1.join(Stream2)([KEYs], function[Stream1.Value, Stream2.Value] transform, TimeWindows) => KStream[OrderId, Option[Order]]
    val ordersPaid = ordersStream.join(paymentsStream)(functionForJoinValues, joinWindow)
        .flatMapValues(_.toIterable)
    // FILTER IS NICE, but I don't want an option -> .filter((orderid, optionOrder) => optionOrder.nonEmpty)
    // so I changed it to flatMapValues => now I will just have the value, and remember is Option is empty, the iterable is non existent


    // ------------------------------------------------- SINK -------------------------------------------------
    // you can now send your result through another topic
    ordersPaid.to(PaidOrdersTopic)

    // ------------------------------------------------- BUILD -------------------------------------------------
    val topology = builder.build()



    def main(args: Array[String]): Unit = {
        List("orders-by-user",
            "discount-profiles-by-user",
            "discounts",
            "orders",
            "payments",
            "paid-orders"
        ).foreach{ topic =>
            println(s"kafka-topics --bootstrap-server localhost:9092 --topic ${topic} --create")
        }
        // this shows the terminal for kafka
        // docker exec -it broker bash
    }
}