package com.github.pyvitto.kafka.producer

import content.bankTransactionProducer
import upickle.default._
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

class BankTransactions {
    @Test
    def newRandomTest(): Unit = {
        val randomProducerRecord: ProducerRecord[String, String] = bankTransactionProducer.newRandomRecord("Vitto")
        val recordKey:String = randomProducerRecord.key()
        val recordValue:String = randomProducerRecord.value()

        assertEquals(recordKey,"Vitto")
        //read String to JSON
        val dataJson = ujson.read(recordValue)
        println(dataJson)
        assertEquals(dataJson("user").str,"Vitto")
        assertTrue( dataJson("balance").num.toLong < 100)

    }

}
