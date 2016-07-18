package sparklyr

object submit {
  def main(args: Array[String]): Unit = {
    System.err.println("Running from spark-submit")

    System.exit(0)
  }
}
