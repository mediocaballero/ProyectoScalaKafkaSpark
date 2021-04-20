package com.pruebas.serializer

class Usuario(var usuarioId: Int, var nombre:String) {
  def getID:Int = usuarioId
  def getNombre:String = nombre
  override def toString:String = "Referencia: " + usuarioId + ", Nombre: " + nombre
}
   