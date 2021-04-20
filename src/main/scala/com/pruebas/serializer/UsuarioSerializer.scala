package com.pruebas.serializer

import java.nio.ByteBuffer
import java.util

import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Serializer

class UsuarioSerializer extends Serializer[Usuario]{
  private val encoding = "UTF8"

  override def configure(map: util.Map[String, _], b: Boolean): Unit = {}

  override def serialize(topic: String, data: Usuario): Array[Byte] = {
    try{
      var nombre_ser = data.getNombre.getBytes(encoding)
      val sizeOfName = nombre_ser.length
      val buf = ByteBuffer.allocate(sizeOfName + 50)
      buf.putInt(data.getID)
      buf.putInt(sizeOfName)
      buf.put(nombre_ser)
      buf.array()   
    }catch{
      case e:Exception => throw new SerializationException("Error al serializar Usuario")
    }
  }

  override def close(): Unit = {}
}
