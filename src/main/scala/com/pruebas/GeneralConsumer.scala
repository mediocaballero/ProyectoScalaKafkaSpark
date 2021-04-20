package com.pruebas

import java.time.Duration
import java.util.Properties

import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.KafkaConsumer
 
object GeneralConsumer { 
  def main(args: Array[String]): Unit = {
    val props: Properties = new Properties()
    props.put("group.id", "test")
    props.put("bootstrap.servers","node1:9092")
    props.put("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")

    //Creamos una instancia de KaskaConsumer
    val consumer = new KafkaConsumer[String, String](props)
    //definimos el topic
    val topics = List("pruebas")

    try{
      //nos subscribimos al topic o a una lista de topics
      consumer.subscribe(topics.asJava)
      while(true){ //evitamos el deprecated con Duration...
        val mensaje = consumer.poll(Duration.ofMillis(10))
        mensaje.asScala.foreach(println)
      }
    }catch{
      case e:Exception => e.printStackTrace()
    }finally {
      consumer.close()
    }
  }
}
