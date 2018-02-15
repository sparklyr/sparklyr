package sparklyr

object Backend {
  /* Leaving this entry for backward compatibility with databricks */
  def main(args: Array[String]): Unit = {
    Shell.main(args)
  }
}
