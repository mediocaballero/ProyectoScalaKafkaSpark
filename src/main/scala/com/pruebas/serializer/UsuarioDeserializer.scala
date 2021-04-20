package com.pruebas.serializer

import java.nio.ByteBuffer
import java.util
import org.apache.kafka.common.serialization.Deserializer

class UsuarioDeserializer extends Deserializer[Usuario]{
  private val encoding = "UTF8"

  override def configure(map: util.Map[String, _], b: Boolean): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): Usuario = {
    val buf = ByteBuffer.wrap(data)
    //recuperamos el id
    val id = buf.getInt()
    val sizeOfName = buf.getInt
    val nombre_ser = new Array[Byte](sizeOfName)
    buf.get(nombre_ser)
    val deserializedName = new String(nombre_ser, encoding)
    new Usuario(id, deserializedName)
  }   

  override def close(): Unit = {}
}
