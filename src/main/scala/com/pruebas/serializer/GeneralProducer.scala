package com.pruebas.serializer

import java.util._

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object GeneralProducer {
  def main(args: Array[String]): Unit = {

      val props:Properties = new Properties()
      props.put("bootstrap.servers", "node1:9092")
      props.put("key.serializer",
        "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer",
        "com.pruebas.serializer.UsuarioSerializer")
      props.put("acks", "1")

    //Creamos una instancia de la clase KafkaProducer, para un tipo Usuario en value
    val producer = new KafkaProducer[String, Usuario](props)
  
    //definimos el topic
    val topic = "pruebas_part"

    try {
      //creamos un bucle para enviar 10 mensajes
      for(i <- 0 to 10) {
          val user = new Usuario(i, "Julia"+i)
          //definimos el mensaje de tipo String,Usuario
          val mensaje = new ProducerRecord[String, Usuario](topic, i.toString, user)
          val metadata = producer.send(mensaje).get()
          printf(s"Enviando mensaje (key=%s, value=%s) ," +
                 s"meta(particion=%d, offset=%d)\n",
                 mensaje.key(), mensaje.value(),
                 metadata.partition(),
                 metadata.offset())
      }
    }catch{
      case e:Exception => e.printStackTrace()
    }finally{
      //cerramos el producer
      producer.close()
    }

  }
}
