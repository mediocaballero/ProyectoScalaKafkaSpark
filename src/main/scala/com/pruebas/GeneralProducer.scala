package com.pruebas

import java.util._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
 
object GeneralProducer {
  def main(args: Array[String]): Unit = {

      val props:Properties = new Properties()
      props.put("bootstrap.servers", "node1:9092")
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
      //creamos un bucle para enviar 10 mensajes
      for(i <- 0 to 10) {
          //creamos el mensaje a partir de ProducerRecord
          val mensaje = new ProducerRecord[String, String](topic,
            null, "Contenido mensaje" + i)

          //enviamos el mensaje y leemos el metadata
          val metadata = producer.send(mensaje).get()
          printf(s"Enviando mensaje (key=%s, value=%s) ," +
                 s"meta(particion=%d, offset=%d, topic=%s)\n",
                 mensaje.key(), mensaje.value(),
                 metadata.partition(),
                 metadata.offset(),
                 metadata.topic())
      }
    }catch{
      case e:Exception => e.printStackTrace()
    }finally{
      //cerramos el producer
      producer.close()
    }




  }
}
