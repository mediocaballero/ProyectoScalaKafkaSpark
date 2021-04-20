package com.pruebas.partitioner

import java.util

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.record.InvalidRecordException

class CustomPartitioner extends Partitioner {

  override def partition(topic: String, key: Any, keyBytes: Array[Byte],
                         value: Any, valueBytes: Array[Byte], cluster: Cluster): Int = {
    //Obtenemos las particiones del topic
    val partitions = cluster.partitionsForTopic("topic_part")
    //total de particiones del topic
    val numPartitions = partitions.size()
    //evaluamos la key para decidir hacia donde dirigimos el mensaje
    if((keyBytes == null) || (!key.isInstanceOf[String])) {
      throw new InvalidRecordException("Todos los mensajes deben tener clave")
    }
    if(key.asInstanceOf[String].startsWith("admon")){
      0
    }else if(key.asInstanceOf[String].startsWith("rrhh")){
      1
    }else{
      2
    }
  }

  override def close(): Unit = {}

  override def configure(map: util.Map[String, _]): Unit = {}
}
