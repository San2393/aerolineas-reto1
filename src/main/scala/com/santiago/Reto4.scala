package com.santiago

import org.apache.spark.sql.DataFrame

class Reto4 {

  //4. ¿Que dias se presentan más retrasos históricamente?
  /**
    * Encuentre que dia de la semana se presentan más demoras,
    * sólo tenga en cuenta los vuelos donde ARR_DELAY > 45min
    *
    * @param ds
    * @return Una lista con tuplas de la forma (DayOfTheWeek, NumberOfDelays) i.e.("Monday",12356)
    */
  def daysWithDelays(ds: DataFrame): List[(String, Long)] = ???

}
