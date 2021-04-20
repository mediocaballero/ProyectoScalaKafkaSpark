package com.pruebas.asincrono

import java.util._
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import scala.concurrent.Promise

object ProducerAsincrono {
  def main(args: Array[String]): Unit = {

    val props:Properties = new Properties()
    props.put("bootstrap.servers", "quickstart.cloudera:9092")
    props.put("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks", "1")

    //Creamos una instancia de la clase KafkaProducer
    val producer = new KafkaProducer[String, String](props)
    //definimos el topic
    val topic = "pruebas"

    try {
      val mensaje = new ProducerRecord[String, String](topic, "Contenido del mensaje asincrono")
      //creamos una instancia de Promise para el valor Future recuperado
      val p = Promise[(RecordMetadata, Exception)]()
      producer.send(mensaje, new Callback {
        override def onCompletion(metadata: RecordMetadata, e: Exception): Unit = {
          println("---Callback, estado: " + p.success((metadata, e)) +
                  ", Offset: " + metadata.offset())
        }
      })

    }catch{
      case e:Exception => e.printStackTrace()
    }finally{
      //cerramos el producer
      producer.close()
    }




  }
}
