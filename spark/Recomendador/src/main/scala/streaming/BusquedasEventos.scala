package streaming

trait BusquedasEventos {
  
  protected val  NUM_EVENTOS_BANNER = 4
  
  def getBanner(edad: Integer, sexo: String): Option[String] =
    (edad, sexo) match {
      case (x, y) if (x >= 0 && x < 10) && sexo.equalsIgnoreCase("m") => Some("0-10m")
      case (x, y) if (x >= 10 && x < 20) && sexo.equalsIgnoreCase("m") => Some("10-20m")
      case (x, y) if (x >= 20 && x < 30) && sexo.equalsIgnoreCase("m") => Some("20-30m")
      case (x, y) if (x >= 30 && x < 40) && sexo.equalsIgnoreCase("m") => Some("30-40m")
      case (x, y) if (x >= 40 && x < 60) && sexo.equalsIgnoreCase("m") => Some("40-60m")
      case (x, y) if (x >= 60 && x < 80) && sexo.equalsIgnoreCase("m") => Some("60-80m")
      case (x, y) if (x >= 80 && x < 100) && sexo.equalsIgnoreCase("m") => Some("80-100m")

      case (x, y) if (x >= 0 && x < 10) && sexo.equalsIgnoreCase("f") => Some("0-10f")
      case (x, y) if (x >= 10 && x < 20) && sexo.equalsIgnoreCase("f") => Some("10-20f")
      case (x, y) if (x >= 20 && x < 30) && sexo.equalsIgnoreCase("f") => Some("20-30f")
      case (x, y) if (x >= 30 && x < 40) && sexo.equalsIgnoreCase("f") => Some("30-40f")
      case (x, y) if (x >= 40 && x < 60) && sexo.equalsIgnoreCase("f") => Some("40-60f")
      case (x, y) if (x >= 60 && x < 80) && sexo.equalsIgnoreCase("f") => Some("60-80f")
      case (x, y) if (x >= 80 && x < 100) && sexo.equalsIgnoreCase("f") => Some("80-100f")
      case _ => None

    }
}