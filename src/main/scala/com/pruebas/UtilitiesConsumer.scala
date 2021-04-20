package com.pruebas

import java.util
import java.util.Properties
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

object UtilitiesConsumer {
  def main(args: Array[String]): Unit = {
   
      val properties = new Properties()
      properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092")
      properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test")
      properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10")
      properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
      properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")

      val consumer = new KafkaConsumer[String, String](properties)
    try {
      //The consumer needs to be subscribed to some topic or partition before making a call to poll.
      consumer.subscribe(util.Arrays.asList("pruebas_part"))

      val recordsFromConsumer = consumer.poll(20000)
      //To find the offset of the latest record read by the consumer, we can retrieve the last ConsumerRecord from the
      // list of records in ConsumerRecords and then call the offset method on that record.
      val recordsFromConsumerList = recordsFromConsumer.asScala.toList

      //Now, this offset is the last offset that is read by the consumer from the topic.
      val lastReadOffset = recordsFromConsumerList.last.offset()
      println(s"Ultimo offset leido por consumer : $lastReadOffset")

      //Now, to find the last offset of the topic, i.e. the offset of the last record present in the topic, we can use the
      // endOffsets method of KafkaConsumer. It gives the last offset for the given partitions. Its return type
      // is Map<TopicPartition, Long>.
      //The last offset of a partition is the offset of the upcoming message, i.e. the offset of the last available message + 1.
      val partitionsAssigned = consumer.assignment()
      val endOffsetsPartitionMap = consumer.endOffsets(partitionsAssigned)
      println(s"Particion asignada : ${partitionsAssigned}")
      println(s"Ultimo offset : ${endOffsetsPartitionMap}")

      //You should call the method assignment only after calling poll on the consumer; otherwise, it will give null as the
      // result. Additionally, the method endOffsets doesn’t change the position of the consumer, unlike seek methods,
      // which do change the consumer position/offset.
      //You can find the current position of the Consumer using:
      val currentPosition = consumer.position(partitionsAssigned.toList.head)
      println(s"Posicion actual : ${currentPosition}")

      //Now that we have with us the last read offset by the Consumer and the endOffset of a partition of the source topic,
      //we can find their difference to find the Consumer lag.
      val consumerLag = endOffsetsPartitionMap.get(partitionsAssigned.head) - lastReadOffset
      println(s"Consumer Lag : ${consumerLag}")

    }catch{
      case e:Exception => e.printStackTrace()
    }finally {
      consumer.close()
    }
  }
}
