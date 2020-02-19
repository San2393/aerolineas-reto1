package com.santiago

import org.apache.spark.sql.DataFrame

class Reto3 {

  //3. Encuentre ¿Cuáles son los números de vuelo (top 20)  que han tenido más cancelaciones y sus causas?

  case class CancelledFlight(number: Int, origin: String, destination: String, cancelled: Long, causes: List[(String, Int)])

  /**
    * Encuentre los vuelos más cancelados y cual es la causa mas frecuente
    * Un vuelo es cancelado si CANCELLED = 1
    * CANCELLATION_CODE A - Airline/Carrier; B - Weather; C - National Air System; D - Security
    *
    * @param ds
    */
  def flightInfo(ds: DataFrame): Seq[CancelledFlight] = ???

}
