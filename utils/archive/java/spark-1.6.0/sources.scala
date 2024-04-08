package sparklyr

object Sources {
  val sources: String =
    scala.io.Source.fromInputStream(getClass.getResourceAsStream("embedded_sources.R"), "UTF-8")
    .getLines
    .mkString("\n")
}
