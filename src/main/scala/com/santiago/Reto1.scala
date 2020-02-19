package com.santiago

import org.apache.spark.sql.Dataset


class Reto1 {

  //1. ¿Cuáles son las aerolíneas más cumplidas y las menos cumplidas de un año en especifico?
  //La respuesta debe incluir el nombre completo de la aerolínea, si no se envia el año debe calcular con
  //toda la información disponible.

  case class AirlineDelay(FL_DATE: String,
                          OP_CARRIER: String,
                          ORIGIN: String,
                          DEST: String,
                          DEP_DELAY: Option[String],
                          ARR_DELAY: Option[String])

  case class AirlineStats(name: String,
                          totalFlights: Long,
                          largeDelayFlights: Long,
                          smallDelayFlights: Long,
                          onTimeFlights: Long)

  /**
    * Un vuelo se clasifica de la siguiente manera:
    * ARR_DELAY < 5 min --- On time
    * 5 > ARR_DELAY < 45min -- small Delay
    * ARR_DELAY > 45min large delay
    *
    * Calcule por cada aerolinea el número total de vuelos durante el año (en caso de no recibir el parametro de todos los años)
    * y el número de ontime flights, smallDelay flighst y largeDelay flights
    *
    * Orderne el resultado por largeDelayFlights, smallDelayFlightsy, ontimeFlights
    *
    * @param ds
    * @param year
    */
  def delayedAirlines(ds: Dataset[AirlineDelay], year: Option[String]): Seq[AirlineStats] = ???

}
