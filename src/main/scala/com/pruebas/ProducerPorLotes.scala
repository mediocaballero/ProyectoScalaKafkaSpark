package com.pruebas

import java.util._

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import java.util.concurrent.Future
  
object ProducerPorLotes {
  def main(args: Array[String]): Unit = {
      //definimos un tamaño para los lotes
      val batchSize:java.lang.Integer = 163000
      val props:Properties = new Properties()
      props.put("bootstrap.servers", "node1:9092")
      props.put("key.serializer",
        "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer",
        "org.apache.kafka.common.serialization.StringSerializer")
      props.put("acks", "1")
     //propiedad que determina el limite de tiempo para enviar el lote
    props.put("linger.ms", "10")
    //establecemos el tamaño del lote
    props.put("batch.size", batchSize)
    //Cuantos reintentos hace el producer en caso de fallo
    props.put("retries", "3")


    //Creamos una instancia de la clase KafkaProducer
    val producer = new KafkaProducer[String, String](props)
    //definimos el topic
    val topic = "pruebas"

    try {
      //creamos un bucle para enviar 10 mensajes
      for(i <- 0 to 10) {
          //creamos el mensaje a partir de ProducerRecord
          val mensaje = new ProducerRecord[String, String](topic, null, "Contenido mensaje" + i)
          val respuesta:Future[RecordMetadata] = producer.send(mensaje)
          //println(respuesta.isDone)
          println("Respuesta:: " +respuesta.get().serializedValueSize() +
                  ", isDone: " + respuesta.isDone)
      }
    }catch{
      case e:Exception => e.printStackTrace()
    }finally{
      //cerramos el producer
      producer.close()
    }




  }
}
