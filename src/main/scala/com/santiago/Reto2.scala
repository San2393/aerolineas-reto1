package com.santiago

import org.apache.spark.sql.DataFrame

class Reto2 {

  // 2. Dado un origen por ejemplo DCA (Washington), ¿Cuáles son destinos y cuantos vuelos presentan durante la mañana, tarde y noche?
  case class FlightsStats(destination: String, morningFlights: Long, afternoonFlights, nightFlights: Long)

  /**
    * Encuentre los destinos a partir de un origen, y de acuerdo a DEP_TIME clasifique el vuelo de la siguiente manera:
    * 00:00 y 8:00 - Morning
    * 8:01 y 16:00 - Afternoon
    * 16:01 y 23:59 - Night
    *
    * @param ds
    * @param origin
    * @return
    */
  def destinations(ds: DataFrame, origin: String): Seq[FlightsStats]


}
