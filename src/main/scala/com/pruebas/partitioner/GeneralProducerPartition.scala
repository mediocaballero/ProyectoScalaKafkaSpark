package com.pruebas.partitioner

import java.util._

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
//kafka-topics --create --zookeeper node1:2181 --replication-factor 1 --partitions 3 --topic topic_part --if-not-exists

object GeneralProducerPartition {
  def main(args: Array[String]): Unit = {

      val props:Properties = new Properties()
      props.put("bootstrap.servers", "node1:9092")
      props.put("key.serializer",
        "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer",
        "org.apache.kafka.common.serialization.StringSerializer")
    props.put("partitioner.class",
      "com.pruebas.partitioner.CustomPartitioner")
      props.put("acks", "1")

    //Creamos una instancia de la clase KafkaProducer
    val producer = new KafkaProducer[String, String](props)
    //definimos el topic
    val topic = "topic_part"

    try {
      //creamos un bucle para enviar 10 mensajes
      for(i <- 0 to 10) {
        val mensaje1 = new ProducerRecord[String, String](topic,
                                                         "admon",
                                                         "Mensaje para admon" + i)
        producer.send(mensaje1)
      }
      for(i <- 0 to 5) {
        val mensaje2 = new ProducerRecord[String, String](topic,
                                                   "rrhh",
                                                  "Mensaje para rrhh" + i)
        producer.send(mensaje2)
      }
      for(i <- 0 to 5) {
        val mensaje3 = new ProducerRecord[String, String](topic,
                                                   "empresa",
                                                  "Mensaje para empresa" + i)
        producer.send(mensaje3)
      }
    }catch{
      case e:Exception => e.printStackTrace()
    }finally{
      //cerramos el producer
      producer.close()
    }




  }
}
